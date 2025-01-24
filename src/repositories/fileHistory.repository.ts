import { inject } from '@loopback/core';
import { DefaultCrudRepository } from '@loopback/repository';
import { DbDataSource } from '../datasources';
import { FileHistory, FileHistoryRelations } from '../models';

export class FileHistoryRepository extends DefaultCrudRepository<
    FileHistory,
    typeof FileHistory.prototype.id,
    FileHistoryRelations
> {
    constructor(
        @inject('datasources.db') dataSource: DbDataSource,
    ) {
        super(FileHistory, dataSource);
    }
}
