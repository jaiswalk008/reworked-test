import { inject } from "@loopback/core";
import { DefaultCrudRepository } from "@loopback/repository";
import { DbDataSource } from "../datasources";
import { GenerateLeadsModel, GenerateLeadsRelations } from "../models";

export class GenerateLeadsRepository extends DefaultCrudRepository<
GenerateLeadsModel,
  typeof GenerateLeadsModel.prototype.id,
  GenerateLeadsRelations
> {
  constructor(@inject("datasources.db") dataSource: DbDataSource) {
    super(GenerateLeadsModel, dataSource);
  }
  // async getGenerateLeadsModelCollection() {
  //   const collection = (this.dataSource.connector as any).collection("GenerateLeadsModel");
  //   return collection;
  // }
}
