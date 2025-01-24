import { inject } from "@loopback/core";
import { DefaultCrudRepository } from "@loopback/repository";
import { DbDataSource } from "../datasources";
import { Feedback,FeedbackRelations } from "../models/feedback.model";
export class FeedbackRepository extends DefaultCrudRepository<
Feedback,
  typeof Feedback.prototype.id,
  FeedbackRelations
> {
  constructor(@inject("datasources.db") dataSource: DbDataSource) {
    super(Feedback, dataSource);
  }
}
