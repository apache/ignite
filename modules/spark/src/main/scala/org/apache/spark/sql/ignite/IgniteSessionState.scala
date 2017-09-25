package org.apache.spark.sql.ignite

import org.apache.spark.sql.{ExperimentalMethods, SparkSession, UDFRegistration}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{QueryExecution, SparkPlanner}
import org.apache.spark.sql.internal.{SQLConf, SessionResourceLoader, SessionState, SharedState}
import org.apache.spark.sql.streaming.StreamingQueryManager
import org.apache.spark.sql.util.ExecutionListenerManager

/**
  */
class IgniteSessionState (
    sharedState: SharedState,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods,
    functionRegistry: FunctionRegistry,
    udfRegistration: UDFRegistration,
    catalog: SessionCatalog,
    sqlParser: ParserInterface,
    analyzer: Analyzer,
    optimizer: Optimizer,
    planner: SparkPlanner,
    streamingQueryManager: StreamingQueryManager,
    listenerManager: ExecutionListenerManager,
    resourceLoader: SessionResourceLoader,
    createQueryExecution: LogicalPlan => QueryExecution,
    createClone: (SparkSession, SessionState) => SessionState)
    extends SessionState(
        sharedState,
        conf,
        experimentalMethods,
        functionRegistry,
        udfRegistration,
        catalog,
        sqlParser,
        analyzer,
        optimizer,
        planner,
        streamingQueryManager,
        listenerManager,
        resourceLoader,
        createQueryExecution,
        createClone) {

}
