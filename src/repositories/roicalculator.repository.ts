import {inject} from '@loopback/core';
import {DefaultCrudRepository} from '@loopback/repository';
import {DbDataSource} from '../datasources';
 import { RoiCalculator , RoiCalculatorRelations } from '../models/roicalculator.model';

export class RoiCalculatorRepository extends DefaultCrudRepository<
RoiCalculator,
  typeof RoiCalculator.prototype.id,
  RoiCalculatorRelations
> {
  constructor(
    @inject('datasources.db') dataSource: DbDataSource,
  ) {
    super(RoiCalculator, dataSource);
  }
}
