import {inject} from '@loopback/core';
import {DefaultCrudRepository} from '@loopback/repository';
import {DbDataSource} from '../datasources';
import { Referral, ReferralRelations} from '../models';

export class ReferralRepository extends DefaultCrudRepository<
  Referral,
  typeof Referral.prototype.id,
  ReferralRelations
> {
  constructor(
    @inject('datasources.db') dataSource: DbDataSource,
  ) {
    super(Referral, dataSource);
  }
}
