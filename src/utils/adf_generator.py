import json

def generate_adf_pipeline_json(src_table_name, table_suffix, is_initial_load=True, is_invalid_hs=False, is_placeholder=False, source_system_initial=None, source_system_daily=None):
    """Generate ADF pipeline JSON for either initial or daily load"""
    if is_placeholder:
        return generate_st_placeholder_pipeline(src_table_name, table_suffix, source_system_initial)
    elif is_invalid_hs:
        return generate_invalid_hs_pipeline(src_table_name, table_suffix, source_system_initial)
    elif is_initial_load:
        return generate_initial_load_pipeline(src_table_name, table_suffix, source_system_initial)
    else:
        return generate_daily_load_pipeline(src_table_name, table_suffix, source_system_initial, source_system_daily)

def generate_st_placeholder_pipeline(src_table_name, table_suffix, source_system_initial=None):
    """Generate ADF pipeline JSON that uses ST_Placeholder to only run the HS part"""
    # Sanitize the table name to ensure no invalid characters
    sanitized_table_name = src_table_name.replace(" ", "_").replace("-", "_")
    pipeline_name = f"pl_HSOnlyWithPlaceholder_{sanitized_table_name}"
    
    # Determine correct HS job name based on source system
    hs_job_name = "HS_Profisee_Daily" if source_system_initial and "Profisee_dev" in source_system_initial else "HS_Full_Initial"
    hs_control_job = "HS_Profisee_Daily_Control" if source_system_initial and "Profisee_dev" in source_system_initial else "HS_Full_Daily_Control"
    
    return {
        "name": pipeline_name,
        "properties": {
            "activities": [
                {
                    "name": "HS_Only_with_Placeholder",
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
                            "pSTJob": "ST_Placeholder",
                            "pHSJob": hs_job_name,
                            "pJobControlSchema": "sandbox",
                            "pJobControlTable": f"temp_control_table_job_{table_suffix}",
                            "pSTTablesControlSchema": "sandbox",
                            "pSTTablesControlTable": f"temp_control_table_st_{table_suffix}",
                            "pHSTablesControlSchema": "sandbox",
                            "pHSTablesControlTable": f"temp_control_table_hs_{table_suffix}",
                            "pLoopJob": hs_control_job,
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
            "annotations": [
                "This pipeline uses ST_Placeholder to skip the Stage part and run only the HS part",
                "Use this after creating the HS table manually to complete the initial load process"
            ]
        }
    }

def generate_invalid_hs_pipeline(src_table_name, table_suffix, source_system_initial=None):
    """Generate ADF pipeline JSON that runs ST job with invalid HS job name"""
    # Sanitize the table name to ensure no invalid characters
    sanitized_table_name = src_table_name.replace(" ", "_").replace("-", "_")
    pipeline_name = f"pl_StageOnlyWithInvalidHS_{sanitized_table_name}"
    
    # Determine correct ST job name based on source system
    st_job_name = "ST_Profisee_Initial" if source_system_initial and "Profisee_dev" in source_system_initial else "ST_Full_Initial"
    hs_control_job = "HS_Profisee_Daily_Control" if source_system_initial and "Profisee_dev" in source_system_initial else "HS_Full_Daily_Control"
    
    return {
        "name": pipeline_name,
        "properties": {
            "activities": [
                {
                    "name": "Stage_Only_Invalid_HS",
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
                            "pSTJob": st_job_name,
                            "pHSJob": "INVALID_HS_JOB_NAME",
                            "pJobControlSchema": "sandbox",
                            "pJobControlTable": f"temp_control_table_job_{table_suffix}",
                            "pSTTablesControlSchema": "sandbox",
                            "pSTTablesControlTable": f"temp_control_table_st_{table_suffix}",
                            "pHSTablesControlSchema": "sandbox",
                            "pHSTablesControlTable": f"temp_control_table_hs_{table_suffix}",
                            "pLoopJob": hs_control_job,
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
            "annotations": [
                "This pipeline intentionally uses an invalid HS job name to run only the Stage part",
                "After this pipeline completes (and fails at the HS stage), create the HS table manually",
                "Then run either the full initial load or the ST_Placeholder job to complete the process"
            ]
        }
    }

def generate_initial_load_pipeline(src_table_name, table_suffix, source_system_initial=None):
    """Generate ADF pipeline JSON for initial load"""
    # Sanitize the table name to ensure no invalid characters
    sanitized_table_name = src_table_name.replace(" ", "_").replace("-", "_")
    pipeline_name = f"pl_StageAndHistoricStageInitialLoad_{sanitized_table_name}"
    
    # Determine correct job names based on source system
    st_job_name = "ST_Profisee_Initial" if source_system_initial and "Profisee_dev" in source_system_initial else "ST_Full_Initial"
    hs_job_name = "HS_Profisee_Daily" if source_system_initial and "Profisee_dev" in source_system_initial else "HS_Full_Initial"
    hs_control_job = "HS_Profisee_Daily_Control" if source_system_initial and "Profisee_dev" in source_system_initial else "HS_Full_Daily_Control"
    
    return {
        "name": pipeline_name,
        "properties": {
            "activities": [
                {
                    "name": "Initial_Stage_and_HS",
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
                            "pSTJob": st_job_name,
                            "pHSJob": hs_job_name,
                            "pJobControlSchema": "sandbox",
                            "pJobControlTable": f"temp_control_table_job_{table_suffix}",
                            "pSTTablesControlSchema": "sandbox",
                            "pSTTablesControlTable": f"temp_control_table_st_{table_suffix}",
                            "pHSTablesControlSchema": "sandbox",
                            "pHSTablesControlTable": f"temp_control_table_hs_{table_suffix}",
                            "pLoopJob": hs_control_job,
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

def generate_daily_load_pipeline(src_table_name, table_suffix, source_system_initial=None, source_system_daily=None):
    """Generate ADF pipeline JSON for daily load"""
    # Sanitize the table name to ensure no invalid characters
    sanitized_table_name = src_table_name.replace(" ", "_").replace("-", "_")
    pipeline_name = f"pl_StageAndHistoricStageDailyLoad_{sanitized_table_name}"
    
    # Determine correct job names based on source system
    st_job_name = "ST_Profisee_Daily" if source_system_daily and "Profisee_dev" in source_system_daily else "ST_Full_Daily"
    hs_job_name = "HS_Profisee_Daily" if source_system_initial and "Profisee_dev" in source_system_initial else "HS_Full_Daily"
    hs_control_job = "HS_Profisee_Daily_Control" if source_system_initial and "Profisee_dev" in source_system_initial else "HS_Full_Daily_Control"
    
    return {
        "name": pipeline_name,
        "properties": {
            "activities": [
                {
                    "name": "Daily_Stage_and_HS",
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
                            "pSTJob": st_job_name,
                            "pHSJob": hs_job_name,
                            "pJobControlSchema": "DWH",
                            "pJobControlTable": "JOB_CONTROL",
                            "pSTTablesControlSchema": "DWH",
                            "pSTTablesControlTable": "CONTROL_TABLE_STAGE",
                            "pHSTablesControlSchema": "DWH",
                            "pHSTablesControlTable": "CONTROL_TABLE_HS",
                            "pLoopJob": hs_control_job,
                            "pLogSchema": "DWH",
                            "pLogTableJobLevel": "JOB_LOG",
                            "pLogTableTableLevel": "JOB_TABLES_LOG",
                            "pIntialLoad": False
                        }
                    }
                },
                {
                    "name": "Stage_Deletes",
                    "type": "ExecutePipeline",
                    "state": "Inactive",
                    "onInactiveMarkAs": "Succeeded",
                    "dependsOn": [
                        {
                            "activity": "Daily_Stage_and_HS",
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
                            "pJobName": st_job_name,
                            "pDataControlTable": "CONTROL_TABLE_STAGE",
                            "pDataControlSchema": "DWH"
                        }
                    }
                },
                {
                    "name": "Update_Delete_flags",
                    "type": "ExecutePipeline",
                    "state": "Inactive",
                    "onInactiveMarkAs": "Succeeded",
                    "dependsOn": [
                        {
                            "activity": "Stage_Deletes",
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
                            "pJobName": hs_job_name,
                            "pDataControlTable": "CONTROL_TABLE_HS",
                            "pDataControlSchema": "DWH",
                            "pStageControlTable": "CONTROL_TABLE_STAGE",
                            "pStageControlSchema": "DWH",
                            "pStageJobName": st_job_name
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
                            "activity": "Update_Delete_flags",
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