import {inject} from '@loopback/core';
import {DefaultCrudRepository} from '@loopback/repository';
import {DbDataSource} from '../datasources';
import {Signup, SignupRelations} from '../models';

export class SignupRepository extends DefaultCrudRepository<
  Signup,
  typeof Signup.prototype.id,
  SignupRelations
> {
  constructor(
    @inject('datasources.db') dataSource: DbDataSource,
  ) {
    super(Signup, dataSource);
  }
}
