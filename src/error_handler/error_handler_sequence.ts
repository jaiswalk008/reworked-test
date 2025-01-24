import {
    RequestContext,
    SequenceHandler,
    FindRoute,
    InvokeMethod,
    ParseParams,
    Reject,
    Send,
    RestBindings,
  } from '@loopback/rest';
  import { inject } from '@loopback/core';
  
  const SequenceActions = RestBindings.SequenceActions;
  
  export class ErrorHandlerSequence implements SequenceHandler {
    constructor(
      @inject(SequenceActions.FIND_ROUTE) protected findRoute: FindRoute,
      @inject(SequenceActions.PARSE_PARAMS) protected parseParams: ParseParams,
      @inject(SequenceActions.INVOKE_METHOD) protected invoke: InvokeMethod,
      @inject(SequenceActions.SEND) public send: Send,
      @inject(SequenceActions.REJECT) public reject: Reject,
    ) {}
  
    async handle(context: RequestContext): Promise<void> {
      const { request, response } = context;
      try {
        const route = this.findRoute(request);
        const args = await this.parseParams(request, route);
        const result = await this.invoke(route, args);
        this.send(response, result);
      } catch (err) {
        // Global error handler
        this.handleError(response, err);
      }
    }
  
    handleError(response: any, error: any) {
      console.error('Global error handler:', error); // Log the error
  
      // Custom handling for specific errors (optional)
      if (error.name === 'UnauthorizedError') {
        response.status(401).send({ msg: 'Unauthorized access', error: error.message });
      } else if (error.name === 'ValidationError') {
        response.status(422).send({ msg: 'Validation error', error: error.message });
      } else {
        // Default to 500 for unhandled errors
        response.status(500).send({
          msg: 'An unexpected error occurred',
          error: error.message,
        });
      }
    }
  }
  