package com.zendesk

import com.zendesk.scalaflow.coders.TupleCoders
import com.zendesk.scalaflow.sugar._

package object scalaflow extends CaseClassOps
  with TupleCoders
  with CoderOps
  with CollectionOps
  with DurationOps
  with KVCollectionOps
  with MiscOps
  with PipelineOps
  with TypeTagOps
  with WrapperOps
