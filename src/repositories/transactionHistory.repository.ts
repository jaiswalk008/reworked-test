import {inject} from '@loopback/core';
import {DefaultCrudRepository} from '@loopback/repository';
import {DbDataSource} from '../datasources';
import { TransactionHistory, TransactionHistoryRelations} from '../models';

export class TransactionHistoryRepository extends DefaultCrudRepository<
TransactionHistory,
    typeof TransactionHistory.prototype.id,
    TransactionHistoryRelations
    > {
    constructor(
        @inject('datasources.db') dataSource: DbDataSource,
    ) {
        super(TransactionHistory, dataSource);
    }
}
