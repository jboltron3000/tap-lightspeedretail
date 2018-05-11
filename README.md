# tap-lightspeedretail

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Lightspeed Retail REST API](https://www.lightspeedhq.com/)
  - [Inventory](https://developers.lightspeedhq.com/retail/tutorials/inventory/)
  - [Order](https://developers.lightspeedhq.com/retail/endpoints/Order/)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

Copyright &copy; 2018 Stitch
