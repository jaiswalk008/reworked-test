import { inject } from "@loopback/core";
import { DefaultCrudRepository } from "@loopback/repository";
import { DbDataSource } from "../datasources";
import { CustomerModels, CustomerModelsRelations } from "../models";

export class CustomerModelsRepository extends DefaultCrudRepository<
CustomerModels,
  typeof CustomerModels.prototype.id,
  CustomerModelsRelations
> {
  constructor(@inject("datasources.db") dataSource: DbDataSource) {
    super(CustomerModels, dataSource);
  }
}
