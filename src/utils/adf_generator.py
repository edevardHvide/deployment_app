import json

def generate_adf_pipeline_json(src_table_name, table_suffix, is_initial_load=True):
    """Generate ADF pipeline JSON for either initial or daily load"""
    if is_initial_load:
        return generate_initial_load_pipeline(src_table_name, table_suffix)
    else:
        return generate_daily_load_pipeline(src_table_name, table_suffix)

def generate_initial_load_pipeline(src_table_name, table_suffix):
    """Generate ADF pipeline JSON for initial load"""
    pipeline_name = f"pl_StageAndHistoricStageInitialLoad_{src_table_name}"
    
    return {
        "name": pipeline_name,
        "properties": {
            "activities": [
                {
                    "name": "Initial Stage and HS",
                    "type": "ExecutePipeline",
                    "dependsOn": [],
                    "policy": {
                        "secureInput": False
                    },
                    "userProperties": [],
                    "typeProperties": {
                        "pipeline": {
                            "referenceName": "pl_framework_StageAndHSLoop",
                            "type": "PipelineReference"
                        },
                        "waitOnCompletion": True,
                        "parameters": {
                            "pStopDate": {
                                "value": "@formatDateTime(addDays(utcNow(),1),'yyyy-MM-dd HH:mm:ss')",
                                "type": "Expression"
                            },
                            "pSTJob": "ST_Full_Initial",
                            "pHSJob": "HS_Full_Initial",
                            "pJobControlSchema": "sandbox",
                            "pJobControlTable": f"temp_control_table_job_{table_suffix}",
                            "pSTTablesControlSchema": "sandbox",
                            "pSTTablesControlTable": f"temp_control_table_st_{table_suffix}",
                            "pHSTablesControlSchema": "sandbox",
                            "pHSTablesControlTable": f"temp_control_table_hs_{table_suffix}",
                            "pLoopJob": "HS_Full_Initial_Control",
                            "pLogSchema": "DWH",
                            "pLogTableJobLevel": "JOB_LOG",
                            "pLogTableTableLevel": "JOB_TABLES_LOG",
                            "pIntialLoad": True
                        }
                    }
                }
            ],
            "folder": {
                "name": "Deployment and initial load"
            },
            "annotations": []
        }
    }

def generate_daily_load_pipeline(src_table_name, table_suffix):
    """Generate ADF pipeline JSON for daily load"""
    pipeline_name = f"pl_StageAndHistoricStageDailyLoad_{src_table_name}"
    
    return {
        "name": pipeline_name,
        "properties": {
            "activities": [
                {
                    "name": "Daily Stage and HS",
                    "type": "ExecutePipeline",
                    "dependsOn": [],
                    "policy": {
                        "secureInput": False
                    },
                    "userProperties": [],
                    "typeProperties": {
                        "pipeline": {
                            "referenceName": "pl_framework_StageAndHSLoop",
                            "type": "PipelineReference"
                        },
                        "waitOnCompletion": True,
                        "parameters": {
                            "pStopDate": {
                                "value": "@formatDateTime(addDays(utcNow(),1),'yyyy-MM-dd HH:mm:ss')",
                                "type": "Expression"
                            },
                            "pSTJob": "ST_Full_Daily",
                            "pHSJob": "HS_Full_Daily",
                            "pJobControlSchema": "sandbox",
                            "pJobControlTable": f"temp_control_table_job_{table_suffix}",
                            "pSTTablesControlSchema": "sandbox",
                            "pSTTablesControlTable": f"temp_control_table_st_{table_suffix}",
                            "pHSTablesControlSchema": "sandbox",
                            "pHSTablesControlTable": f"temp_control_table_hs_{table_suffix}",
                            "pLoopJob": "HS_Full_Daily_Control",
                            "pLogSchema": "DWH",
                            "pLogTableJobLevel": "JOB_LOG",
                            "pLogTableTableLevel": "JOB_TABLES_LOG",
                            "pIntialLoad": False
                        }
                    }
                },
                {
                    "name": "Stage Deletes",
                    "type": "ExecutePipeline",
                    "state": "Inactive",
                    "onInactiveMarkAs": "Succeeded",
                    "dependsOn": [
                        {
                            "activity": "Daily Stage and HS",
                            "dependencyConditions": [
                                "Succeeded"
                            ]
                        }
                    ],
                    "policy": {
                        "secureInput": False
                    },
                    "userProperties": [],
                    "typeProperties": {
                        "pipeline": {
                            "referenceName": "pl_framework_StageDeleteLoop",
                            "type": "PipelineReference"
                        },
                        "waitOnCompletion": True,
                        "parameters": {
                            "pJobName": "ST_Full_Daily",
                            "pDataControlTable": "CONTROL_TABLE_STAGE",
                            "pDataControlSchema": "DWH"
                        }
                    }
                },
                {
                    "name": "Update Delete flags",
                    "type": "ExecutePipeline",
                    "state": "Inactive",
                    "onInactiveMarkAs": "Succeeded",
                    "dependsOn": [
                        {
                            "activity": "Stage Deletes",
                            "dependencyConditions": [
                                "Succeeded"
                            ]
                        }
                    ],
                    "policy": {
                        "secureInput": False
                    },
                    "userProperties": [],
                    "typeProperties": {
                        "pipeline": {
                            "referenceName": "pl_framework_UpdateDeleteFlag",
                            "type": "PipelineReference"
                        },
                        "waitOnCompletion": True,
                        "parameters": {
                            "pJobName": "HS_Full_Daily",
                            "pDataControlTable": "CONTROL_TABLE_HS",
                            "pDataControlSchema": "DWH",
                            "pStageControlTable": "CONTROL_TABLE_STAGE",
                            "pStageControlSchema": "DWH",
                            "pStageJobName": "ST_Full_Daily"
                        }
                    }
                },
                {
                    "name": "BK_preload",
                    "type": "ExecutePipeline",
                    "state": "Inactive",
                    "onInactiveMarkAs": "Succeeded",
                    "dependsOn": [
                        {
                            "activity": "Update Delete flags",
                            "dependencyConditions": [
                                "Succeeded"
                            ]
                        }
                    ],
                    "policy": {
                        "secureInput": False
                    },
                    "userProperties": [],
                    "typeProperties": {
                        "pipeline": {
                            "referenceName": "pl_BKPreload_Test",
                            "type": "PipelineReference"
                        },
                        "waitOnCompletion": True
                    }
                }
            ],
            "folder": {
                "name": "Scheduling"
            },
            "annotations": []
        }
    } 