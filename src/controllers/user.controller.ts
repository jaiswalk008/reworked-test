import {
  Count,
  CountSchema,
  Filter,
  FilterExcludingWhere,
  repository,
  Where,
} from '@loopback/repository';
import {
  post,
  param,
  get,
  getModelSchemaRef,
  patch,
  put,
  del,
  requestBody,
  response,
} from '@loopback/rest';
import {Promo, Signup} from '../models';
import {PromoRepository, SignupRepository} from '../repositories';
import { TokenServiceBindings, UserRepository } from '@loopback/authentication-jwt';
import { TokenService, authenticate } from '@loopback/authentication';
import { inject } from "@loopback/core";
export class UserController {
  constructor(
    @inject(TokenServiceBindings.TOKEN_SERVICE)
  public jwtService: TokenService,
    @repository(SignupRepository)
    public signupRepository: SignupRepository,
    @repository(PromoRepository)
    public promoRepository: PromoRepository
  ) {}

  @authenticate('jwt')
  @post('/signups')
  @response(200, {
    description: 'Signup model instance',
    content: {'application/json': {schema: getModelSchemaRef(Signup)}},
  })
  async create(
    @requestBody({
      content: {
        'application/json': {
          schema: getModelSchemaRef(Signup, {
            title: 'NewSignup',
            exclude: ['id'],
          }),
        },
      },
    })
    signup: Omit<Signup, 'id'>,
  ): Promise<Signup> {
    return this.signupRepository.create(signup);
  }
  @authenticate("jwt")
  @post("/addPromo")
  @response(200, {
    description: "Add Promotion",
    content: { "application/json": { schema: getModelSchemaRef(Signup) } },
  })
  async addPromo(
    @requestBody({
      content: {
        "application/json": {
          schema: getModelSchemaRef(Promo, {
            title: "NewPromo",
            exclude: ["id"],
          }),
        },
      },
    })
    promo: Omit<Promo, "id">
  ): Promise<Promo> {
    
    let x = this.promoRepository.create(promo);
    console.log("x = ",x)
    return x
  }
  @authenticate("jwt")
  @get("/signups/count")
  @response(200, {
    description: 'Signup model count',
    content: {'application/json': {schema: CountSchema}},
  })
  async count(
    @param.where(Signup) where?: Where<Signup>,
  ): Promise<Count> {
    return this.signupRepository.count(where);
  }
  @authenticate('jwt')
  @get('/signups')
  @response(200, {
    description: 'Array of Signup model instances',
    content: {
      'application/json': {
        schema: {
          type: 'array',
          items: getModelSchemaRef(Signup, {includeRelations: true}),
        },
      },
    },
  })
  async find(
    @param.filter(Signup) filter?: Filter<Signup>,
  ): Promise<Signup[]> {
    return this.signupRepository.find(filter);
  }
  @authenticate('jwt')
  @patch('/signups')
  @response(200, {
    description: 'Signup PATCH success count',
    content: {'application/json': {schema: CountSchema}},
  })
  async updateAll(
    @requestBody({
      content: {
        'application/json': {
          schema: getModelSchemaRef(Signup, {partial: true}),
        },
      },
    })
    signup: Signup,
    @param.where(Signup) where?: Where<Signup>,
  ): Promise<Count> {
    return this.signupRepository.updateAll(signup, where);
  }
  @authenticate('jwt')
  @get('/signups/{id}')
  @response(200, {
    description: 'Signup model instance',
    content: {
      'application/json': {
        schema: getModelSchemaRef(Signup, {includeRelations: true}),
      },
    },
  })
  async findById(
    @param.path.number('id') id: number,
    @param.filter(Signup, {exclude: 'where'}) filter?: FilterExcludingWhere<Signup>
  ): Promise<Signup> {
    return this.signupRepository.findById(id, filter);
  }
  @authenticate('jwt')
  @patch('/signups/{id}')
  @response(204, {
    description: 'Signup PATCH success',
  })
  async updateById(
    @param.path.number('id') id: number,
    @requestBody({
      content: {
        "application/json": {
          schema: getModelSchemaRef(Signup, { partial: true }),
        },
      },
    })
    signup: Signup
  ): Promise<void> {
    await this.signupRepository.updateById(id, signup);
  }
  @authenticate("jwt")
  @put("/signups/{id}")
  @response(204, {
    description: "Signup PUT success",
  })
  async replaceById(@param.path.number("id") id: number, @requestBody() signup: Signup): Promise<void> {
    await this.signupRepository.replaceById(id, signup);
  }
  @authenticate("jwt")
  @del("/signups/{id}")
  @response(204, {
    description: "Signup DELETE success",
  })
  async deleteById(@param.path.number("id") id: number): Promise<void> {
    await this.signupRepository.deleteById(id);
  }
}
