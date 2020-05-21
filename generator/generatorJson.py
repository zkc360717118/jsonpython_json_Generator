#!/usr/bin/python
# -*- coding: UTF-8 -*-

import json
import time

from generator.Module import Tool


class WorkBench:
    # constructor
    def __init__(self, module: str):
        tool = Tool()
        self.module = module

        # form three stages under the skeleton
        self.input_stage = tool.getStageTemp(module=module, stgNm="DATA_SOURCE", stgSeq=0,
                                             stgDesc="read data from source")
        self.transform_stage = tool.getStageTemp(module=module, stgNm="DATA_TRANSFORM", stgSeq=1,
                                                 stgDesc="transformation for " + module)
        self.output_stage = tool.getStageTemp(module=module, stgNm="DATA_OUTPUT", stgSeq=2,
                                              stgDesc="export data  for " + module)

    def addSubtasksForReadStage(self, *tasks):
        self.input_stage["processTasks"] = tasks

    def addSubtasksForTransformStage(self, *tasks):
        self.transform_stage["processTasks"] = tasks

    def addSubtasksForOutputStage(self, *tasks):
        self.output_stage["processTasks"] = tasks

    def formFinalJson(self, procNm, subProcNm, description, createdBy,effStatus="y",effDt="",projectNm="REGINSIGHT_APAC",legalEntity="@@@@Group",formNm="NA",projectid=0,sheduleNm="NA",source="ETL",contractTypeId="1"):
        tool = Tool()
        finalJson = tool.setMainInfo(self.module, procNm, subProcNm, description, effStatus, createdBy,effDt,projectNm,legalEntity,formNm,projectid,sheduleNm,source,contractTypeId)
        finalJson["processStages"] = [self.input_stage,self.transform_stage,self.output_stage]
        return finalJson;



# print(test.input_stage)

# get module info
# x = Module()
# moduleJson = x.dtsJson


# create differenct kinds of stages
# stages = list()
# st1 = x.getStageTemp()
# st1["stgNm"] = "read"
# st2 = x.getStageTemp()
# st2["stgNm"] = "transform"
# st3 = x.getStageTemp()
# st3["stgNm"] = "write"
# create subtasks for read stage
# readTask = x.getTaskTemp("read")
# st1["processTasks"] = readTask

# create subtasks for tranform stage
# transformTasks = list()
# transfromT1 = x.getTaskTemp("reformat")
# transfromT2 = x.getTaskTemp("sort")
# transformTasks.append(transfromT1)
# transformTasks.append(transfromT2)
# st2["processTasks"] = transformTasks

# create subtasks for write stage
# writeTask = x.getTaskTemp("write")
# st3["processTasks"] = writeTask

# print(task2)

# print(readTask)
# print(transfromT2)
# print(json.dumps(writeTask))
# print(st1)
# print(st2)
# print(st3)

# assemble all stages
# stages.append(st1)
# stages.append(st2)
# stages.append(st3)
# moduleJson["processStages"] = stages
# print(json.dumps(moduleJson,indent=2))
