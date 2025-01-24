import {
    param,
    post,
    get,
    Request,
    RequestBody,
    requestBody,
    Response,
    RestBindings,
} from "@loopback/rest";
import { repository } from "@loopback/repository";
import { AdminEventsRepository, CustomerRepository, CustomerModelsRepository, CustomerIndustryRepository } from "../repositories";
import { TransactionHistoryRepository } from "../repositories";
import { inject } from "@loopback/core";
import stripeClient from "../services/stripeClient";
import { TokenServiceBindings, UserRepository } from '@loopback/authentication-jwt';
import { TokenService, authenticate } from '@loopback/authentication';
import { stripePayment, changeSubscriptionPlan, updateStripeDefaultPayment } from '../helper/stripe-payment';
import { runPythonScript, UploadS3, CustomerModelsService, FileUploadProvider } from "../services";
import { stripPaymentInfo, updateChildAccountPricingPlan } from "../helper/stripe-payment";
import { TransactionHistory, TransactionHistoryRelations } from "../models";
// This is your Stripe CLI webhook secret for testing your endpoint locally.
import { paymentTypes } from "../helper/constant";
import { getFilterSort } from "../helper/filter-sort";
const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET;

//const @repository(CustomerRepository)

export class StripeCallbackController {
    constructor(
        @inject(TokenServiceBindings.TOKEN_SERVICE)
        public jwtService: TokenService,
        @repository(CustomerRepository)
        public customerRepository: CustomerRepository,
        @repository(TransactionHistoryRepository)
        public transactionHistoryRepository: TransactionHistoryRepository,
        @repository(AdminEventsRepository)
        protected adminEventsRepository: AdminEventsRepository,
        @repository(CustomerModelsRepository)
        protected customerModelsRepository: CustomerModelsRepository,
        @repository(CustomerIndustryRepository)
        protected customerIndustryRepository: CustomerIndustryRepository


    ) { }



    @authenticate('jwt')
    @post('/stripe_call_back', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Callback for Stripe',
            },
        },
    })
    async stripeCallBack(
        @param.header.string('stripe-signature') stripeSignature: string,
        @requestBody({
            description: 'Raw Body',      // Description can be anything
            required: true,
            content: {
                'application/json': {       // Make sure this matches the POST request type
                    'x-parser': 'raw',        // This is the key to skipping parsing
                    schema: { type: 'object' },
                },
            },
        }) body: Buffer,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<void> {

        // Retrieve the event by verifying the signature using the raw body and secret.
        let event;

        try {
            event = stripeClient.webhooks.constructEvent(
                body,
                stripeSignature,
                webhookSecret
            );
        } catch (err) {
            console.log(err);
            console.log(`⚠️  Webhook signature verification failed.`);
            console.log(
                `⚠️  Check the env file and enter the correct webhook secret.`
            );
            response.sendStatus(400);
        }
        // Extract the object from the event.
        const dataObject = event.data.object;
        let findbyemail;


        // Handle the event
        // Review important events for Billing webhooks
        // https://stripe.com/docs/billing/webhooks
        // Remove comment to see the various objects sent for this sample
        switch (event.type) {
            case 'customer.subscription.updated':
                console.log("Stripe Controller: Subscription updated event (changing plans) for stripe customer id ", dataObject.customer);
                findbyemail = await this.customerRepository.find({
                    fields: { login_history: false, file_history: false },
                    where: { stripe_customer_id: dataObject.customer }
                });
                findbyemail[0].pricing_plan.stripe_price_id = dataObject.data.price.id;
                await this.customerRepository.update(findbyemail[0], { where: { stripe_customer_id: dataObject.customer } });

                break;
            case 'invoice.paid':
                console.log("Stripe Controller: Invoice paid for email ", dataObject.customer_email);
                findbyemail = await this.customerRepository.find({
                    fields: { login_history: false, file_history: false },
                    where: { email: dataObject.customer_email }
                });
                findbyemail[0].pricing_plan.stripe_subscription_status = "active";
                // Used to provision services after the trial has ended.
                // The status of the invoice will show up as paid. Store the status in your
                // database to reference when a user accesses your service to avoid hitting rate limits.
                await this.customerRepository.update(findbyemail[0], { where: { email: dataObject.customer_email } });
                break;
            case 'invoice.payment_failed':
                console.log("Stripe Controller: Payment failed for email ", dataObject.customer_email);
                findbyemail = await this.customerRepository.find({
                    fields: { login_history: false, file_history: false },
                    where: { email: dataObject.customer_email }
                });
                findbyemail[0].pricing_plan.stripe_subscription_status = "payment_due";
                await this.customerRepository.update(findbyemail[0], { where: { email: dataObject.customer_email } });

                // If the payment fails or the customer does not have a valid payment method,
                //  an invoice.payment_failed event is sent, the subscription becomes past_due.
                // Use this webhook to notify your user that their payment has
                // failed and to retrieve new card details.
                break;
            case 'customer.subscription.deleted':
                console.log("Stripe Controller: Payment failed for stripe customer id ", dataObject.customer);
                findbyemail = await this.customerRepository.find({
                    fields: { login_history: false, file_history: false },
                    where: { stripe_customer_id: dataObject.customer }
                });
                findbyemail[0].pricing_plan.stripe_subscription_status = "canceled";
                findbyemail[0].pricing_plan.subscription_id = '';
                await this.customerRepository.update(findbyemail[0], { where: { stripe_customer_id: dataObject.customer } });
/*                 if (event.request != null) {
                    // handle a subscription canceled by your request
                    // from above.
                } else {
                    // handle subscription canceled automatically based
                    // upon your subscription settings.
                }
                break;
 */            default:
            // Unexpected event type
        }

        response.sendStatus(200);

    }

    @authenticate('jwt')
    @post('/upsert-subscription-coupon', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Create Stripe Subscription Coupon',
            },
        },
    })
    async createStripeSubscriptionCoupon(
        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                        required: ['coupon', 'percent'],
                        properties: {
                            coupon: {
                                type: 'string'
                            },
                            percent: {
                                type: 'number'
                            },
                            newPercent: {
                                type: 'number'
                            }
                        }
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<Response<any, Record<string, any>>> {
        // check if email is of admin
        const adminEmail = request.headers.email as string
        const admin = await this.customerRepository.findOne({ where: { email: adminEmail, role: 'admin' } });
        if (!admin) return response.status(403).send('Request should be made by admin')
        const lowerCaseCoupon = request.body.coupon.toLowerCase()
        if (request.body.newPercent) {
            try {
                await stripeClient.coupons.del(lowerCaseCoupon);
                const coupon = await stripeClient.coupons.create({
                    duration: 'once',
                    id: lowerCaseCoupon,
                    percent_off: request.body.newPercent,
                });
                return response.send(coupon)
            } catch (e) {
                console.log(e)
                return response.status(400).send(e.message)
            }
        } else {
            const coupon = await stripeClient.coupons.create({
                duration: 'once',
                id: lowerCaseCoupon,
                percent_off: request.body.percent,
            });
            return response.send(coupon);
        }
    }

    @post('/couponPercentage', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Create Stripe Subscription Coupon',
            },
        },
    })
    async getPercentageFromCoupon(
        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                        required: ['coupon'],
                        properties: {
                            coupon: {
                                type: 'string'
                            }
                        }
                    },
                },
            },
        })
        request: any,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<Response<any, Record<string, any>>> {
        console.log("Stripe Controller --> Getting percentage from coupon", JSON.stringify(request));
        const lowerCaseCoupon = request.coupon.toLowerCase()
        try {
            const coupon = await stripeClient.coupons.retrieve(lowerCaseCoupon)
            return response.send({ "percent_off": coupon.percent_off })

        } catch (e) {
            console.log(e.message)
        }
        return response.status(400).send('Invalid coupon code');
    }

    @authenticate('jwt')
    @post('/create-subscription', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Create Stripe Subscription',
            },
        },
    })
    async createStripeSubscription(


        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                        required: ['email', 'stripe_customer_id', 'stripe_price_id'],
                        properties: {
                            email: {
                                type: 'string',
                            },
                            stripe_customer_id: {
                                type: 'string'
                            },
                            stripe_payment_method_id: {
                                type: 'string'
                            },
                            stripe_price_id: {
                                type: 'string'
                            },
                            coupon: {
                                type: 'string'
                            }
                        }
                    },
                },
            },
        })
        request: any,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<Response<any, Record<string, any>>> {

        // As of May '23 we're disabling the feature where billing being was anchored to beginning of the month
        // var date = new Date();
        // let backdate_start_date = new Date(date.getFullYear(), date.getMonth(), 1).getTime() / 1000;
        // let billing_cycle_anchor = new Date(date.getFullYear(), date.getMonth()+1, 1).getTime() / 1000;

        const customer = await this.customerRepository.findOne({ where: { email: request.email } })
        if (!customer) return response.status(400).send('Customer not found')
        if (request.stripe_payment_method_id) {
            await updateStripeDefaultPayment(customer, request.stripe_payment_method_id, this.customerRepository);
        }
        const isCouponAlreadyReedemed = customer.isCouponRedeemed
        // Create the subscription
        let subscriber: any = {
            customer: request.stripe_customer_id,
            items: [{ price: request.stripe_price_id }],
            // As of May '23 we're disabling the feature where billing being was anchored to beginning of the month
            // backdate_start_date: backdate_start_date,
            // billing_cycle_anchor: billing_cycle_anchor,
            expand: ['latest_invoice.payment_intent']
        }
        let subscription;
        const lowerCaseCoupon = request.coupon?.toLowerCase()
        if (lowerCaseCoupon && customer.referrer && lowerCaseCoupon == customer.referrer.toLowerCase() && !isCouponAlreadyReedemed) {
            subscriber.coupon = lowerCaseCoupon
            subscription = await stripeClient.subscriptions.create(subscriber);
            customer.isCouponRedeemed = true
            await this.customerRepository.updateById(customer.id, customer)
        } else {
            subscription = await stripeClient.subscriptions.create(subscriber);
        }
        const findbyemail = await this.customerRepository.find({
            fields: { login_history: false, file_history: false },
            where: { email: request.email }
        });

        let price_id = request.stripe_price_id;
        findbyemail[0].pricing_plan = {
            "subscription_id": subscription.id,
            "current_period_end": subscription.current_period_end,
            "stripe_price_id": price_id,
            "plan": process.env[price_id],
            "stripe_subscription_status": subscription.status,
            "stripe_product_id": subscription.items.data[0].price.product,
            "stripe_payment_method_id": subscription.latest_invoice.payment_intent?.payment_method || request.stripe_payment_method_id,
            "stripe_invoice_id": subscription.latest_invoice.id,
            "stripe_payment_intent_status": subscription.latest_invoice.payment_intent?.status,
            "stripe_payment_intent_client_secret": subscription.latest_invoice.payment_intent?.client_secret,
            'resume_paused_subscription_at': null,
            'start_paused_subscription_at': null,
            'auto_debit': true
        }
        let cust: any = findbyemail[0];
        delete cust['file_history'];
        delete cust['login_history'];
        
        // update all child account with pricing plan
        await updateChildAccountPricingPlan(findbyemail[0], this.customerRepository);

        await this.customerRepository.update(findbyemail[0], { where: { email: request.email } })

        try {
            let last4Digit = await stripPaymentInfo(cust);
            cust = { ...cust, cc_last4digit: last4Digit?.cardInfo?.last4, brand: last4Digit?.cardInfo?.brand }
        } catch (error) {
            console.error("Error in stripPaymentInfo function", error)
        }
        return response.send(cust);
    }

    @authenticate('jwt')
    @post('/retry-invoice', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Retry Stripe payment',
            },
        },
    })
    async retryInvoice(

        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                        required: ['email', 'stripe_customer_id', 'stripe_payment_method_id', 'stripe_invoice_id'],
                        properties: {
                            email: {
                                type: 'string',
                            },
                            stripe_customer_id: {
                                type: 'string'
                            },
                            stripe_payment_method_id: {
                                type: 'string'
                            },
                            stripe_invoice_id: {
                                type: 'string'
                            }
                        }
                    },
                },
            },
        })
        request: any,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<Response<any, Record<string, any>>> {
        const findbyemail = await this.customerRepository.findOne({
            fields: { login_history: false, file_history: false },
            where: { email: request.email }
        });
        if (!findbyemail) {
            return response.send(null);
        }
        // attaching default method id
        await updateStripeDefaultPayment(findbyemail, request.stripe_payment_method_id, this.customerRepository)

        const invoice = await stripeClient.invoices.retrieve(request.stripe_invoice_id, {
            expand: ['payment_intent'],
        });


        console.log(`Stripe Controller --> Subscription created for email ${request.email} and subscription response is ${JSON.stringify(invoice)}`);
        findbyemail.pricing_plan.stripe_invoice_id = invoice.id;
        findbyemail.pricing_plan.stripe_payment_intent_status = invoice.payment_intent.status;
        findbyemail.pricing_plan.stripe_payment_intent_client_secret = invoice.payment_intent.client_secret;
        // findbyemail.pricing_plan.stripe_payment_method_id = invoice.payment_intent.payment_method;
        let cust: any = findbyemail;
        
        // update all child account with pricing plan
        await updateChildAccountPricingPlan(findbyemail, this.customerRepository);

        await this.customerRepository.update(findbyemail, { where: { email: request.email } })

        return response.send(cust);

    }
    @authenticate('jwt')
    @post('/cancel-subscription', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Cancel Stripe Subscription',
            },
        },
    })
    async cancelSubscription(

        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                        required: ['email', 'stripe_subscription_id'],
                        properties: {
                            email: {
                                type: 'string',
                            },
                            reason: {
                                type: 'string',
                            },
                            stripe_subscription_id: {
                                type: 'string',
                            }
                        }
                    },
                },
            },
        })
        request: any,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<Response<any, Record<string, any>>> {
        // Delete the subscription
        const deletedSubscription = await stripeClient.subscriptions.del(
            request.stripe_subscription_id
        );
        const findbyemail = await this.customerRepository.find({
            fields: { login_history: false, file_history: false },
            where: { email: request.email }
        });
        findbyemail[0].pricing_plan.stripe_subscription_status = deletedSubscription.status;
        findbyemail[0].pricing_plan.subscription_id = '';
        const subscriptionLog = findbyemail[0].subscription_log && findbyemail[0].subscription_log.length ? findbyemail[0].subscription_log : [];
        const subLog = {
            reason: request.reason,
            action: 'cancel',
            timestamp: new Date(),
        }
        subscriptionLog.push(subLog)
        findbyemail[0].subscription_log = subscriptionLog;

        let cust: any = findbyemail[0];
        delete cust['file_history'];
        delete cust['login_history'];

        // update all child account with pricing plan
        await updateChildAccountPricingPlan(findbyemail[0], this.customerRepository);
        await this.customerRepository.update(findbyemail[0], { where: { email: request.email } })

        return response.send(cust);

    }
    @authenticate('jwt')
    @post('/update-subscription', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Update Stripe Subscription (i.e. change plans)',
            },
        },
    })
    async updateSubscription(

        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                        required: ['stripe_subscription_id', 'stripe_price_id'],
                        properties: {
                            stripe_subscription_id: {
                                type: 'string',
                            },
                            stripe_price_id: {
                                type: 'string'
                            }
                        }
                    },
                },
            },
        })
        request: any,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<Response<any, Record<string, any>>> {
        const subscription = await stripeClient.subscriptions.retrieve(
            request.stripe_subscription_id
        );
        const updatedSubscription = await stripeClient.subscriptions.update(
            request.stripe_subscription_id,
            {
                cancel_at_period_end: false,
                items: [
                    {
                        id: subscription.items.data[0].id,
                        price: request.stripe_price_id,
                    },
                ],
            }
        );

        return response.send(updatedSubscription);

    }
    @authenticate('jwt')
    @post('/test', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Create Stripe Subscription',
            },
        },
    })
    async test(

        @requestBody()
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<void> {
        const findbyemail = await this.customerRepository.find({ where: { email: 'ghanashyam.p@gmail.com' } })
        console.log(JSON.stringify(findbyemail));


    }


    @authenticate('jwt')
    @post('/buy-credit', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Buy row credit',
            },
        },
    })
    async buyCredits(

        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                        required: ['no_of_credits', 'total_cost', 'type'],
                        properties: {
                            no_of_credits: {
                                type: 'number',
                            },
                            total_cost: {
                                type: 'number'
                            },
                            type: {
                                type: 'string'
                            }
                        }
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<void> {

        let msg = "";
        let data = {};
        let statusCode = 200;
        let responseToSend = {
            data,
            msg,
        };
        const sourceType = request.body.type || 'lead_sorting';
        let totalAmount = request.body.total_cost;
        if (totalAmount) {

            let metaData = request.body;
            let userEmail;
            let noOfCredits = request.body.no_of_credits;
            try {

                userEmail = request.headers.email as string;
                if (!userEmail) {
                    statusCode = 400;
                    msg = "Headers are missing";
                }
                else if (userEmail) {

                    const customerData = await this.customerRepository.findOne({ where: { email: userEmail } })
                    if (customerData && customerData.stripe_customer_id) {
                        const args = {
                            totalAmount, metaData, noOfCredits, email: userEmail, payment_type: `Buy Row Credits for ${sourceType}`,
                            invoiceItemDescription: "Buy credits",
                            sourceType
                        };
                        let response = await stripePayment(args, customerData, this.customerRepository, this.transactionHistoryRepository
                            , this.adminEventsRepository)
                        data = response.data;
                        msg = response.msg;
                        statusCode = response.statusCode;
                    } else {
                        statusCode = 400;
                        if (!customerData)
                            msg = "Invalid email id"
                        else {
                            msg = "Stipe Id is missing"
                        }
                    }
                }
            }
            catch (error) {
                console.error("error in buy credit api", error)
                statusCode = 400;
                msg = error.message;
            }
        }
        else {
            statusCode = 500;
            msg = "Invalid amount";
        }
        
        responseToSend = {
            data,
            msg
        };
        response.status(statusCode).send(responseToSend);
    }



    @authenticate('jwt')
    @post('/invoice', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Buy row credit',
            },
        },
    })
    async listInvoice(

        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<void> {

        let msg = "Data found successfully";
        let data = [];

        let statusCode = 200;
        let responseToSend = {};
        let userEmail;
        let dataArrayToSend = [];
        try {

            userEmail = request.headers.email as string;
            if (!userEmail) {
                statusCode = 400;
                msg = "Headers are missing";
            }
            else if (userEmail) {

                const customerData = await this.customerRepository.findOne({ where: { email: userEmail } })
                if (customerData && customerData.stripe_customer_id) {

                    const invoiceList = await stripeClient.invoices.list({ customer: customerData.stripe_customer_id, limit: 100 });
                    data = invoiceList ? invoiceList.data : [];
                    if (data && data.length) {
                        dataArrayToSend = data.map((invoice: any) => ({
                            id: invoice.id,
                            amount_paid: invoice.amount_paid,
                            created: invoice.created,
                            hosted_invoice_url: invoice.hosted_invoice_url,
                            subscription: invoice.subscription,
                            type: invoice.subscription ? "Subscription" : invoice?.metadata?.payment_type ? invoice?.metadata?.payment_type : "Row Credits"
                        }));
                    }

                } else {
                    statusCode = 400;
                    if (!customerData)
                        msg = "Invalid email id"
                    else {
                        msg = "Stipe Id is missing"
                    }
                }
            }
        }
        catch (error) {
            console.error("error in buy credit api", error)
            statusCode = 400;
            msg = error.message;
        }

        responseToSend = {
            data: dataArrayToSend,
            msg,
        };
        response.status(statusCode).send(responseToSend);
    }



    @authenticate('jwt')
    @post('/pause-subscription', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Pause subscription',
            },
        },
    })
    async pauseSubscription(

        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                        required: ['pause_period_days'],
                        properties: {
                            pause_period_days: {
                                type: 'number',
                            },
                            reason: {
                                type: 'string',
                            },
                        }
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<void> {

        let msg = "Stripe subscription paused successfully";
        let data = {};
        let statusCode = 200;
        let responseToSend = {
            data,
            msg,
        };
        let userEmail;
        try {

            userEmail = request.headers.email as string;
            if (!userEmail) {
                statusCode = 400;
                msg = "Headers are missing";
            }
            else if (userEmail) {

                if (request?.body?.email) {
                    let adminData = await this.customerRepository.findOne({
                        fields: ['email'],
                        where: { role: 'admin', email: userEmail },
                    });
                    if (adminData) {
                        userEmail = request.body.email as string;
                    } else {
                        response.status(400).send({ msg: 'Invalid request', statusCode: 400, error: 'This feature is for Admin only' });
                    }
                }

                const customerData = await this.customerRepository.findOne({ where: { email: userEmail } })
                if (customerData && customerData.stripe_customer_id) {
                    let dt = new Date();
                    dt.setDate(dt.getDate() + request.body.pause_period_days);
                    const updatedTIme = Math.round(dt.getTime() / 1000);

                    const stripee = await stripeClient.subscriptions.update(customerData.pricing_plan.subscription_id, {
                        pause_collection: {
                            behavior: 'mark_uncollectible',
                            resumes_at: updatedTIme
                        }
                    });
                    customerData.pricing_plan.start_paused_subscription_at = new Date();
                    customerData.pricing_plan.resume_paused_subscription_at = dt;
                    const subscriptionLog = customerData.subscription_log && customerData.subscription_log.length ? customerData.subscription_log : [];
                    const subLog = {
                        reason: request.body.reason,
                        action: 'pause',
                        timestamp: new Date(),
                    }
                    subscriptionLog.push(subLog)
                    customerData.subscription_log = subscriptionLog;
                    // update all child account with pricing plan
                    await updateChildAccountPricingPlan(customerData, this.customerRepository);
                    await this.customerRepository.update(customerData, { where: { email: customerData.email } })
                    data = customerData;
                } else {
                    statusCode = 400;
                    if (!customerData)
                        msg = "Invalid email id"
                    else {
                        msg = "Stipe Id is missing"
                    }
                }
            }
        }
        catch (error) {
            console.error("error in pause subscription", error)
            statusCode = 400;
            msg = error.message;
        }

        responseToSend = {
            data,
            msg
        };
        response.status(statusCode).send(responseToSend);
    }

    @authenticate('jwt')
    @post('/resume-subscription', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Resume subscription',
            },
        },
    })
    async resumeSubscription(

        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<void> {

        let msg = "Stripe subscription resumed successfully";
        let data = {};
        let statusCode = 200;
        let responseToSend = {
            data,
            msg,
        };
        let userEmail;
        try {

            userEmail = request.headers.email as string;
            if (!userEmail) {
                statusCode = 400;
                msg = "Headers are missing";
            }
            else if (userEmail) {

                if (request?.body?.email) {
                    let adminData = await this.customerRepository.findOne({
                        fields: ['email'],
                        where: { role: 'admin', email: userEmail },
                    });
                    if (adminData) {
                        userEmail = request.body.email as string;
                    } else {
                        response.status(400).send({ msg: 'Invalid request', statusCode: 400, error: 'This feature is for Admin only' });
                    }
                }

                const customerData = await this.customerRepository.findOne({ where: { email: userEmail } })
                if (customerData && customerData.stripe_customer_id) {

                    const stripee = await stripeClient.subscriptions.update(customerData.pricing_plan.subscription_id, {
                        pause_collection: null
                    });
                    customerData.pricing_plan.start_paused_subscription_at = null;
                    customerData.pricing_plan.current_period_end = stripee?.current_period_end;
                    customerData.pricing_plan.resume_paused_subscription_at = null;
                    // update all child account with pricing plan
                    await updateChildAccountPricingPlan(customerData, this.customerRepository);
                    await this.customerRepository.update(customerData, { where: { email: customerData.email } })
                    data = customerData;
                    // console.log("stripeestripee",stripee)

                } else {
                    statusCode = 400;
                    if (!customerData)
                        msg = "Invalid email id"
                    else {
                        msg = "Stipe Id is missing"
                    }
                }
            }
        }
        catch (error) {
            console.error("error in resume subscription", error)
            statusCode = 400;
            msg = error.message;
        }

        responseToSend = {
            data,
            msg
        };
        response.status(statusCode).send(responseToSend);
    }


    @authenticate('jwt')
    @post('/paymentMethod')
    async updateCustomerPaymentMethod(
        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                        required: ['payment_method_id'],
                        properties: {
                            payment_method_id: {
                                type: 'string',
                            },
                        }
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<any> {

        let data = {};
        let statusCode = 200;
        let msg = '';
        try {
            const userEmail = request.headers.email as string;
            const paymentMethodId = request.body.payment_method_id;
            if (!userEmail) {
                statusCode = 400;
                msg = "Headers are missing";
            }
            else if (userEmail) {

                const customerData = await this.customerRepository.findOne({ where: { email: userEmail } })
                if (customerData && customerData.stripe_customer_id) {
                    await updateStripeDefaultPayment(customerData, paymentMethodId, this.customerRepository);
                    msg = "Payment updated successfully"
                } else {
                    statusCode = 400;
                    msg = customerData ? "Stipe Id is missing" : "Invalid email id"
                }
            }
        } catch (error) {
            statusCode = 400;
            msg = error.message;
            console.error(error);
        }
        response.status(statusCode).send({ msg, data });
    }

    @post('/resume-subscription-event', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Resume subscription event',
            },
        },
    })
    async resumeSubscriptionEvent(

        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<void> {
        const previousAttritube = request.body?.data?.previous_attributes;
        if (previousAttritube) {
            const behaviorValue = previousAttritube?.pause_collection?.behavior;
            if (behaviorValue == "mark_uncollectible") {
                const subObj = request.body.data.object;
                const customerId = subObj.customer;
                let customerData = await this.customerRepository.findOne({
                    where: {
                        stripe_customer_id: customerId
                    },
                });
                if (customerData) {
                    customerData.pricing_plan.start_paused_subscription_at = null;
                    customerData.pricing_plan.resume_paused_subscription_at = null;
                    customerData.pricing_plan.current_period_end = subObj.current_period_end;
                    // update all child account with pricing plan
                    await updateChildAccountPricingPlan(customerData, this.customerRepository);
                    await this.customerRepository.update(customerData, { where: { email: customerData.email } })
                    // console.log("subObj", subObj)
                }
            }
        }
    }




    @authenticate('jwt')
    @post('/model-payment', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Buy row credit',
            },
        },
    })
    async modelPayment(

        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                        required: ['no_of_credits', 'total_cost'],
                        properties: {
                            stripe_payment_method_id: {
                                type: 'string'
                            },
                            model_id: {
                                type: 'string'
                            },
                        }
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<void> {

        let msg = "";
        let data = {};
        let statusCode = 200;
        let responseToSend = {
            data,
            msg,
        };

        try {

            let totalAmount = 45;
            let metaData = request.body;
            let noOfCredits = request.body?.no_of_credits;
            let modelId = request.body?.model_id;
            const email = request.headers.email as string;

            if (!email) {
                statusCode = 400;
                msg = "Headers are missing";
            }
            else if (email) {

                const customerData = await this.customerRepository.findOne({ where: { email } })
                if (customerData && customerData.stripe_customer_id) {
                    if (request.body.stripe_payment_method_id) {
                        // attaching default method id
                        await updateStripeDefaultPayment(customerData, request.body.stripe_payment_method_id, this.customerRepository);
                    }
                    const args = {
                        totalAmount, metaData, noOfCredits, email, payment_type: "Model Payment",
                        invoiceItemDescription: "New model creation"
                    };
                    let response = await stripePayment(args, customerData, this.customerRepository, this.transactionHistoryRepository
                        , this.adminEventsRepository)
                    data = response?.data;
                    msg = response?.msg;
                    statusCode = response?.statusCode;
                    if (statusCode == 200) {
                        data = customerData
                        const customerModel = await this.customerModelsRepository.findOne({
                            where: {
                                or: [
                                    { and: [{ id: modelId }, { email }, { status: 1 }] },
                                    { and: [{ id: modelId }, { email }, { status: 2 }] },
                                ]
                            }
                        })
                        const customerIndustry = await this.customerIndustryRepository.findOne({
                            where: { email }
                        })
                        if (customerModel && customerIndustry) {
                            customerModel.payment_status = 1;
                            await this.customerModelsRepository.update(customerModel);
                            const moduleType = customerModel?.type == 'lead_sorting' ? 'v1.1' : 'v2';
                            const modelDetails = {
                                modelName: customerModel.name,
                                modelDescription: customerModel.description,
                                defaultModel: customerModel.default,
                                industryType: customerIndustry.industry_type,
                                industrialProfile: customerModel.industry_profile,
                                featureColumns: customerModel?.insights?.feature_columns,
                                moduleType,
                                industryProfileId: customerModel?.industry_profile_id
                            };
                            const existingOrginalFileName = customerModel.vendor_list_url;
                            if (moduleType == "v2") {
                                CustomerModelsService.fileValidateService(this.customerRepository, this.customerModelsRepository, this.customerIndustryRepository, email, existingOrginalFileName)
                                // CustomerModelsService.criteriaGenerationService(this.customerRepository, this.customerModelsRepository, email, existingOrginalFileName)
                            } else
                                CustomerModelsService.leadSortingModelCreationFileValidateService(this.customerRepository, this.customerModelsRepository, this.customerIndustryRepository, email, existingOrginalFileName, modelDetails)
                            try {
                                let last4Digit = await stripPaymentInfo(customerData);
                                data = { ...data, cc_last4digit: last4Digit?.cardInfo?.last4, brand: last4Digit?.cardInfo?.brand }
                            } catch (error) {
                                console.error("Error in stripPaymentInfo function", error)
                            }
                        }
                    }
                } else {
                    statusCode = 400;
                    if (!customerData)
                        msg = "Invalid email id"
                    else {
                        msg = "Stipe Id is missing"
                    }
                }
            }
        }
        catch (error) {
            console.error("error in buy credit api", error)
            statusCode = 400;
            msg = error.message;
        }

        responseToSend = {
            data,
            msg
        };
        response.status(statusCode).send(responseToSend);
    }


    @authenticate('jwt')
    @post('/change-plan', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Buy row credit',
            },
        },
    })
    async changePlan(

        @requestBody({
            content: {
                'application/json': {
                    schema: {
                        type: 'object',
                        required: ['new_plan_id', 'email'],
                        properties: {
                            new_plan_id: {
                                type: 'string'
                            },
                            email: {
                                type: 'string'
                            },
                        }
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<void> {

        let msg = "";
        let data = {};
        let statusCode = 200;

        try {
            const email = request.body.email;
            const newPlanId = request.body?.new_plan_id
            if (!email) {
                statusCode = 400;
                msg = "Headers are missing";
            }
            else if (email) {

                const customerData = await this.customerRepository.findOne({ where: { email } })
                const customerId = customerData?.stripe_customer_id;
                if (customerData && customerId) {

                    const startImmediately = true;
                    const responseObj = await changeSubscriptionPlan(customerId, newPlanId, startImmediately);
                    statusCode = responseObj.status;
                    msg = responseObj.msg;
                    const subscription = responseObj.data;
                    if (responseObj?.success) {
                        const priceId = subscription?.plan?.id;
                        const pricingPlanObj = customerData.pricing_plan;
                        // update customer pricing plan obj
                        customerData.pricing_plan = {
                            ...pricingPlanObj,
                            "subscription_id": subscription.id,
                            "current_period_end": subscription.current_period_end,
                            "stripe_price_id": priceId,
                            "plan": process.env[priceId],
                            "stripe_subscription_status": subscription.status,
                            "stripe_product_id": subscription.items.data[0].price.product,
                            "stripe_invoice_id": subscription.latest_invoice
                        }
                        // update all child account with pricing plan
                        await updateChildAccountPricingPlan(customerData, this.customerRepository);
                        await this.customerRepository.update(customerData, { where: { email } })
                        const { login_history, file_history, ...rest } = customerData;
                        data = rest;
                    }

                } else {
                    statusCode = 400;
                    msg = !customerData ? 'Invalid email id' : "Stipe Id is missing";
                }
            }
        }
        catch (error) {
            console.error("error in buy credit api", error)
            statusCode = 400;
            msg = error.message;
        }

        response.status(statusCode).send({
            data,
            msg
        });
    }

    @get('/get-failed-postpaid-payments', {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Create Stripe Subscription Coupon',
            },
        },
    })
    async getFailedPostPaidPayments(
        @inject(RestBindings.Http.REQUEST) request: any,
        @inject(RestBindings.Http.RESPONSE) response: Response
    ): Promise<Response<(TransactionHistory & TransactionHistoryRelations)[]>> {
        try {
            let where: any = { "payment_type": paymentTypes.POSTPAID_BILLING }
            let order: string = 'transaction_date DESC'

            const email: any = request.query.email;
            const sortName: any = request.query.sortName;
            const type: any = request.query.type;
            const page: any = request.query.page || 1;

            if (email) {
                where['email'] = email
            }

            const filtertype = "postpaid"
            const filterSort = getFilterSort({ where, filtertype, sortName, type })

            where = filterSort.where
            if (filterSort.order !== '') {
                order = filterSort.order
            }

            const limit = 10;
            const offset = (page - 1) * limit;

            const totalCountPromise = this.transactionHistoryRepository.count(where);

            const dataPromise = this.transactionHistoryRepository.find({
                where: where,
                order: [order],
                limit: limit, // This is the 'limit'
                skip: offset, // This is the 'offset'
            });
            const [totalCount, data] = await Promise.all([totalCountPromise, dataPromise]);
            const length = totalCount?.count

            // const pageData = data.splice( (page-1) * 10, 10)
            //   return {
            //     length: length,
            //     data: data
            //   };

            // const data = await this.transactionHistoryRepository.find({where: { and : [ {"payment_type": paymentTypes.POSTPAID_BILLING}, ]}})
            return response.status(200).send({ length, data, msg: "Data Fetched SuccessFully" });
        } catch (e) {
            console.log(e.message)
        }
        return response.status(400).send({ data: [], msg: "Something went wrong" });
    }

}


