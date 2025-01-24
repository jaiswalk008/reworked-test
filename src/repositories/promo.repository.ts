import {inject} from '@loopback/core';
import {DefaultCrudRepository} from '@loopback/repository';
import {DbDataSource} from '../datasources';
import {Promo, PromoRelations} from '../models';

export class PromoRepository extends DefaultCrudRepository<
  Promo,
  typeof Promo.prototype.id,
  PromoRelations
> {
  constructor(
    @inject('datasources.db') dataSource: DbDataSource,
  ) {
    super(Promo, dataSource);
  }
}
