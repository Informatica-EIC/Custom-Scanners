# Model Templates

used as a place to provide templates for common elements for creating custom models for EDC.

the contents of each file can be copied/pasted into your custom scanner model

# File Description

* `model_template.xml` a skeleton for a custom model.  use this to create a new custom model from scratch, with the most important parts at the top `<packageName>`, `<packageLabel>`, `<version>`.   It also has placeholders for adding classes, attributes & associations
* `class_template.xml` - class templates for commonly used classes - derived from `core.DataSet`, `core.DataElement` and `core.DataSource`  - these are especially useful for lineage & connection assignments
* `association_template.xml` - for parent-child associations.  Also for dataflow - but that is not required (if you use core.DataSet & core.DataElement superClasses)
* `attribute_template.xml` - for attribute definitions

