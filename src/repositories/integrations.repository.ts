import {inject} from '@loopback/core';
import {DefaultCrudRepository} from '@loopback/repository';
import {DbDataSource} from '../datasources';
import { Integrations, IntegrationsRelations} from '../models';

export class IntegrationsRepository extends DefaultCrudRepository<
Integrations,
  typeof Integrations.prototype.id,
  IntegrationsRelations
> {
  constructor(
    @inject('datasources.db') dataSource: DbDataSource,
  ) {
    super(Integrations, dataSource);
  }
}
