import { inject } from "@loopback/core";
import { DefaultCrudRepository } from "@loopback/repository";
import { DbDataSource } from "../datasources";
import { Survey, SurveyRelations } from "../models";

export class SurveyRepository extends DefaultCrudRepository<
Survey,
  typeof Survey.prototype.id,
  SurveyRelations
> {
  constructor(@inject("datasources.db") dataSource: DbDataSource) {
    super(Survey, dataSource);
  }
}
