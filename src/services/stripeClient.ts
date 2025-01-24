
const stripeClient = require("stripe")(process.env.STRIPE_SECRET_KEY);
//const stripeClient = require("stripe")("sk_test_51LGQcVFOF500L1IZaldRR0wY9nIgjPGXmetiSz5obYgFhkKBofBUkWDkgK6RrFGfCYmGLzMejMLFw4tUX6CGBaNq001MovLHJJ");

export default stripeClient