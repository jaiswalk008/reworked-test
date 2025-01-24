export const getFilterSort = ({
  where,
  filtertype,
  sortName,
  type,
  
}: {
  where: any;
  filtertype: string;
  sortName: string;
  type: string;
  
}, defaultValue= true) => {
  let localWhere = where;
  let order = "";

  let validatenumber = 7;
  if (filtertype == "leadsorting" || filtertype == "leadsorting model") {
    validatenumber = 6;
  } else if (
    filtertype == "leadgeneration" ||
    filtertype == "leadgeneration model"
  ) {
    validatenumber = 2;
  }
  sortName = sortName && sortName.toLowerCase();
  if (sortName && type) {
    if (type === "7") {
      if (sortName === "status") {
        if (filtertype=== "postpaid") {
            localWhere["error"] = false;
        } else {
            localWhere["status"] = validatenumber;
        }
      } else {
        if (filtertype=== "postpaid") {
            localWhere["error"] = true;
        } else {
            localWhere["status"] = { neq: validatenumber };
        }
      }
    } else {
      if (sortName === "filename") {
        if (
          filtertype == "leadgeneration model" ||
          filtertype == "leadsorting model"
        ) {
          if (type === "1") {
            order = "vendor_list_url ASC";
          } else {
            order = "vendor_list_url DESC";
          }
        } else {
          if (type === "1") {
            order = "filename ASC";
          } else {
            order = "filename DESC";
          }
        }
      } else if (sortName === "email") {
        if (type === "1") {
          order = "email ASC";
        } else {
          order = "email DESC";
        }
      } else if (sortName === "model_name") {
        if (
          filtertype == "leadgeneration model" ||
          filtertype == "leadsorting model"
        ) {
          if (type === "1") {
            order = "name ASC";
          } else {
            order = "name DESC";
          }
        } else {
          if (type === "1") {
            order = "model_name ASC";
          } else {
            order = "model_name DESC";
          }
        }
      } else if (sortName === "upload_date") {
        if (
          filtertype == "leadgeneration model" ||
          filtertype == "leadsorting model" ||
          filtertype === "leadgeneration"
        ) {
          if (type === "1") {
            order = "created_at ASC";
          } else {
            order = "created_at DESC";
          }
        } else if (filtertype=== "postpaid") {
            if (type === "1") {
                order = "transaction_date ASC";
              } else {
                order = "transaction_date DESC";
              }
        } else {
          if (type === "1") {
            order = "upload_date ASC";
          } else {
            order = "upload_date DESC";
          }
        }
      } else if (sortName === "record_count") {
        if (filtertype === "leadgeneration") {
          if (type === "1") {
            order = "lead_count ASC";
          } else {
            order = "lead_count DESC";
          }
        } else {
          if (type === "1") {
            order = "record_count ASC";
          } else {
            order = "record_count DESC";
          }
        }
      } else if (sortName === "amount_spent") {
        if (type === "1") {
          order = "amount_spent ASC";
        } else {
          order = "amount_spent DESC";
        }
      } else if (sortName === "status") {
        if (type === "1") {
          order = "status ASC";
        } else {
          order = "status DESC";
        }
      } else if (sortName === "error") {
        if (type === "1") {
          order = "error ASC";
        } else {
          order = "error DESC";
        }
      } else {
        if (filtertype=== "postpaid") {
            localWhere["error"] = true;
        } else if (filtertype=== "leadgeneration") {
            localWhere["status"] = { neq: validatenumber };
            localWhere["error"] = { neq: null };
        } else if(defaultValue){
            localWhere["status"] = { neq: validatenumber };
        }
      }
    }
  } else {
    if (filtertype=== "postpaid") {
        localWhere["error"] = true;
    } else if (filtertype=== "leadgeneration") {
        localWhere["status"] = { neq: validatenumber };
        localWhere["error"] = { neq: null };
    } else if(defaultValue){
        localWhere["status"] = { neq: validatenumber };
    }
  }

  return { where: localWhere, order: order };
};
