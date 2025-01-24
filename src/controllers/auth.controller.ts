import { inject } from '@loopback/core';
import {
  TokenServiceBindings,
  MyUserService,
  UserServiceBindings,
  UserRepository,
  Credentials,
} from '@loopback/authentication-jwt';

import { authenticate, TokenService } from '@loopback/authentication';
import { SecurityBindings, securityId, UserProfile } from '@loopback/security';
import { repository } from '@loopback/repository';
import { get, post, requestBody, SchemaObject } from '@loopback/rest';


const CredentialsSchema: SchemaObject = {
  type: 'object',
  required: ['email', 'password'],
  properties: {
    email: {
      type: 'string',
      format: 'email',
    },
    name: {
      type: 'string',
      minLength: 3,
    },
  },
};

export const CredentialsRequestBody = {
  description: 'The input of login function',
  required: true,
  content: {
    'application/json': { schema: CredentialsSchema },
  },
};


export class AuthController {
  constructor(@inject(TokenServiceBindings.TOKEN_SERVICE)
  public jwtService: TokenService,
    @inject(UserServiceBindings.USER_SERVICE)
    public userService: MyUserService,
    @inject(SecurityBindings.USER, { optional: true })
    public user: UserProfile,
    @repository(UserRepository) protected userRepository: UserRepository,) { }

  @post('/users/login', {
    responses: {
      '200': {
        description: 'Token',
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
                token: {
                  type: 'string',
                },
              },
            },
          },
        },
      },
    },
  })
  async login(
    @requestBody(CredentialsRequestBody) credentials: UserProfile,
  ): Promise<{ token: string }> {
    // ensure the user exists, and the password is correct
    // convert a User object into a UserProfile object (reduced set of properties)

    // create a JSON Web Token based on the user profile
    const token = await this.jwtService.generateToken(credentials);
    return { token };
  }

  @authenticate('jwt')
  @get('/whoAmI', {
    responses: {
      '200': {
        description: 'Return current user',
        content: {
          'application/json': {
            schema: {
              type: 'string',
            },
          },
        },
      },
    },
  })
  async whoAmI(): Promise<string> {
    return "Authenticated";
  }
}
