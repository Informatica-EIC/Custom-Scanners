# JDBC based custom scanner

this scanner can be used where the standard EDC JDBC Scanner does not work either at all, or as expected.

known examples where the generic JDBC scanner does not work.

* Athena - different types are used for Tables & Views 
* Denodo - denodo does not have the concept of schemas - all tables in all databases were always extracted, causing duplicate table name errors
* GemFire - notes to be added

### How it works

* reuses the `com.bla.bla.bla.Relational` model package - so the results look like out-of-box dbms scans
* profiling will not be possble (yet, until it is supported with custom scans)
* includes a completely generic scanner for JDBC - with parameters to control many options (more than MITI offers for JDBC) - this is the default (superclass)
* database specific sub-classes can over-ride any functions that operate differently - e.g. denodo for catalog/schema extracts + querying denodo for the internal lineage relationships + custom lineage to the original source objects
* view sql parsing is not currently included, but is something that will be possible
