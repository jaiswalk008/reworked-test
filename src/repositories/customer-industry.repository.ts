import {inject} from '@loopback/core';
import {DefaultCrudRepository} from '@loopback/repository';
import {DbDataSource} from '../datasources';
import {CustomerIndustry, CustomerIndustryRelations} from '../models';

export class CustomerIndustryRepository extends DefaultCrudRepository<
  CustomerIndustry,
  typeof CustomerIndustry.prototype.id,
  CustomerIndustryRelations
> {
  constructor(
    @inject('datasources.db') dataSource: DbDataSource,
  ) {
    super(CustomerIndustry, dataSource);
  }
}
