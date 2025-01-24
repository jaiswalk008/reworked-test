import { inject } from "@loopback/core";
import { DefaultCrudRepository } from "@loopback/repository";
import { DbDataSource } from "../datasources";
import { AdminEvents, AdminEventsRelations } from "../models";

export class AdminEventsRepository extends DefaultCrudRepository<
  AdminEvents,
  typeof AdminEvents.prototype.id,
  AdminEventsRelations
> {
  constructor(@inject("datasources.db") dataSource: DbDataSource) {
    super(AdminEvents, dataSource);
  }
}
