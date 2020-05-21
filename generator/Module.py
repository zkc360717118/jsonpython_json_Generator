# noinspection PyTypeChecker
import time, json


class Tool:
    # 初始化第一层json，也就是module
    def __init__(self):
        pass

    def __baseTaskTemp(self):
        return {
            "module": "",
            "stgNm": "",
            "taskNm": "",
            "taskDesc": "",
            "taskSeq": 0,
            "taskType": "",
            "depdTask": "",
            "effDt": "",
            "effStatus": "Y",
            "processTaskParams": [
                {
                    "module": "",
                    "stgNm": "",
                    "taskNm": "",
                    "paramNm": "in_data",
                    "paramVal": "",
                    "effStatus": "Y",
                    "effDt": "",
                    "taskSeq": "",
                },
                {
                    "module": "",
                    "stgNm": "",
                    "taskNm": "",
                    "paramNm": "",
                    "paramVal": "",
                    "effStatus": "Y",
                    "effDt": "",
                    "taskSeq": "",
                },
                {
                    "module": "",
                    "stgNm": "",
                    "taskNm": "",
                    "paramNm": "out_data",
                    "paramVal": "",
                    "effStatus": "Y",
                    "effDt": "",
                    "taskSeq": "",
                }
            ],
            "logClltFlg": "n",
            "statClltFlg": "N",
            "cacheFlg": "n",
            "persistType": "P",
        }

    def __baseReadStageTaskTemp(self):
        basetemp = self.__baseTaskTemp()
        # remove 1 out of 3 elements under processTaskParams
        del (basetemp["processTaskParams"][1])
        basetemp["processTaskParams"][0]["paramNm"] = "in_data"

        basetemp["processTaskParams"][0]["derivedParams"] = [{
            "dsetNm": "",
            "source": "RDBMS",
            "dsetDesc": "",
            "dbConNm": "ORA_REG_META",
            "dbType": "Oracle",
            "qryStr": "",
        }]
        basetemp["processTaskParams"][1]["paramNm"] = "out_data"
        return basetemp

    # get stage template
    def getStageTemp(self, module, stgNm, stgSeq, stgDesc):
        currentYmd = self.getCurrentTime('%Y-%m-%d')
        return {
            "moduel": module,
            "stgNm": stgNm,
            "setSeq": stgSeq,
            "stgDesc": stgDesc,
            "effDt": currentYmd,
            "effStatus": "Y",
            "processTasks": [],
            "logClltFlg": "N",
            "statClltFlg": "N",
            "cacheFlg": "N",
        }

    # # get task temp
    # def getTaskTemp(self, task: str):
    #
    #
    #         wirteTaskJsonPart = self.__baseTaskTemp()
    #         wirteTaskJsonPart["processTaskParams"][1]["derivedParams"] = {
    #             "dsetNm": "",
    #             "source": "RDBMS",
    #             "dsetDesc": "",
    #             "dbConNm": "ORA_REG_META",
    #             "dbType": "Oracle",
    #             "tblNm": "",
    #             "saveMode": "Append",
    #         }
    #         return wirteTaskJsonPart

    #
    def getTaskForTransStage(self, taskType: str, module, taskNm, taskDesc, taskSeq, depdTask,
                             processParamVal, stgNm="DATA_TRANSFORM", effStatus="Y", logClltFlg="N",
                             statClltFlg="N", cacheFlg="N", persistType="P", trfmExpression=""):
        # 增加参数验证功能
        if taskType.upper() not in ["LOOKUP", "DEDUP", "SORT", "REFORMAT", "SORT_WITH_GROUPS", "AGGREGATE", "UNION"]:
            raise Exception("check the task name, not valid:" + taskType)

        # ..todo
        taskType = taskType.upper()
        effDt = self.getCurrentTime(False)
        effDt2 = self.getCurrentTime(True)
        j = {
            "module": module,
            "stgNm": stgNm,
            "taskNm": taskNm,
            "taskDesc": taskDesc,
            "taskSeq": taskSeq,
            "taskType": taskType.upper(),
            "depdTask": depdTask,
            "effDt": effDt,
            "effStatus": effStatus,
            "processTaskParams": [],
            "logClltFlg": logClltFlg,
            "statClltFlg": statClltFlg,
            "cacheFlg": cacheFlg,
            "persistType": persistType,
        }

        # json needs to be modified for certain types of task
        if taskType == "SORT_WITH_GROUPS":
            tmp = list()
            for paramNm in ["in_data", "majorKey", "majorKeyOrder", "minorKey", "minorKeyOrder", "out_data"]:
                paramValResult = processParamVal[paramNm]

                tinyJson = {
                    "module": module,
                    "stgNm": stgNm,
                    "taskNm": taskNm,
                    "paramNm": paramNm,
                    "paramVal": paramValResult,
                    "effStatus": effStatus,
                    "effDt": effDt,
                    "taskSeq": str(taskSeq)
                }
                tmp.append(tinyJson)

            j["processTaskParams"] = tmp
        elif taskType == "AGGREGATE":
            tmp = list()
            for paramNm in ["in_data", "agg", "group_by", "out_data"]:
                paramValResult = processParamVal[paramNm]

                tinyJson = {
                    "module": module,
                    "stgNm": stgNm,
                    "taskNm": taskNm,
                    "paramNm": paramNm,
                    "paramVal": paramValResult,
                    "effStatus": effStatus,
                    "effDt": effDt,
                    "taskSeq": str(taskSeq)
                }
                tmp.append(tinyJson)

            j["processTaskParams"] = tmp
        elif taskType == "UNION":
            tmp = list()
            for paramNm in ["in_data", "out_data"]:
                paramValResult = processParamVal[paramNm]
                tinyJson = {
                    "module": module,
                    "stgNm": stgNm,
                    "taskNm": taskNm,
                    "paramNm": paramNm,
                    "paramVal": paramValResult,
                    "effStatus": effStatus,
                    "effDt": effDt,
                    "taskSeq": str(taskSeq)
                }
                tmp.append(tinyJson)

            j["processTaskParams"] = tmp

        elif taskType in ["SORT", "DEDUP", "REFORMAT"]:
            processParamNmIdentifier = {
                "DEDUP": "keys",
                "SORT": "sort",
                "REFORMAT": "transform",
            }
            processParamNm = processParamNmIdentifier.get(taskType)
            tinyJson = [{
                "module": module,
                "stgNm": stgNm,
                "taskNm": taskNm,
                "paramNm": "in_data",
                "paramVal": processParamVal["in_data"],
                "effStatus": effStatus,
                "effDt": effDt,
                "taskSeq": str(taskSeq),
            },
                {
                    "module": module,
                    "stgNm": stgNm,
                    "taskNm": taskNm,
                    "paramNm": processParamNm,
                    "paramVal": processParamVal[processParamNm],
                    "effStatus": effStatus,
                    "effDt": effDt2,
                    "taskSeq": taskSeq,
                },
                {
                    "module": module,
                    "stgNm": stgNm,
                    "taskNm": taskNm,
                    "paramNm": "out_data",
                    "paramVal": processParamVal["out_data"],
                    "effStatus": effStatus,
                    "effDt": effDt2,
                    "taskSeq": taskSeq,
                }]
            j["processTaskParams"] = tinyJson
        else:
            raise Exception("task type is not valid, pls check !!!")

        # reformat need and transformation Expression ,which means further modification
        if taskType == "REFORMAT":
            j["processTaskParams"][1]["transformationDetails"] = [{
                "trmNm": processParamVal["transform"],
                "trfmExpression": trfmExpression
            }]
        return j

    def getTaskForReadStage(self, module, taskNm, taskDesc, taskSeq, intputparamVal, dsetDesc, qryStr, outputparamVal,
                            dbType="Oracle", stgNm="DATA_SOURCE", dbConNm="ORA_REG_META", source="RDBMS", effStatus="Y",
                            logClltFlg="N", statClltFlg="N", cacheFlg="N", persistType="P"):

        effDt = self.getCurrentTime(False)
        effDt2 = self.getCurrentTime(True)
        j = {
            "module": module,
            "stgNm": stgNm,
            "taskNm": taskNm,
            "taskDesc": taskDesc,
            "taskSeq": taskSeq,
            "taskType": "READ",
            "depdTask": "NA",
            "effDt": effDt,
            "effStatus": effStatus,
            "processTaskParams": [{
                "module": module,
                "stgNm": stgNm,
                "taskNm": taskNm,
                "paramNm": "in_data",
                "paramVal": intputparamVal,
                "effStatus": effStatus,
                "effDt": effDt2,
                "taskSeq": str(taskSeq),
                "derivedParams": {
                    "dsetNm": intputparamVal,
                    "source": source,
                    "dsetDesc": dsetDesc,
                    "dbConNm": dbConNm,
                    "dbType": dbType,
                    "qryStr": qryStr
                }
            }, {
                "module": module,
                "stgNm": stgNm,
                "taskNm": taskNm,
                "paramNm": "out_data",
                "paramVal": outputparamVal,
                "effStatus": effStatus,
                "effDt": effDt2,
                "taskSeq": str(taskSeq)
            }],
            "logClltFlg": logClltFlg,
            "statClltFlg": statClltFlg,
            "cacheFlg": cacheFlg,
            "persistType": persistType
        }
        return j

    def getTaskForOutputStage(self, module, taskNm, taskDesc, depdTask, intputparamVal, dsetDesc, tblNm,
                              outputparamVal,saveMode="Append",taskSeq=0,
                              dbType="Oracle", stgNm="DATA_EXPORT", dbConNm="ORA_REG_META", source="RDBMS",
                              effStatus="Y",
                              logClltFlg="N", statClltFlg="N", cacheFlg="N", persistType="P"):

        effDt = self.getCurrentTime(False)
        effDt2 = self.getCurrentTime(True)
        j = {
            "module": module,
            "stgNm": stgNm,
            "taskNm": taskNm,
            "taskDesc": taskDesc,
            "taskSeq": taskSeq,
            "taskType": "WRITE",
            "depdTask": depdTask,
            "effDt": effDt,
            "effStatus": effStatus,
            "processTaskParams": [{
                "module": module,
                "stgNm": stgNm,
                "taskNm": taskNm,
                "paramNm": "in_data",
                "paramVal": intputparamVal,
                "effStatus": effStatus,
                "effDt": effDt2,
                "taskSeq": str(taskSeq)
            },
            {
                "module": module,
                "stgNm": stgNm,
                "taskNm": taskNm,
                "paramNm": "out_data",
                "paramVal": outputparamVal,
                "effStatus": effStatus,
                "effDt": effDt2,
                "taskSeq": str(taskSeq),
                "derivedParams": {
                    "dsetNm": outputparamVal,
                    "source": source,
                    "dsetDesc": dsetDesc,
                    "dbConNm": dbConNm,
                    "dbType": dbType,
                    "tblNm": tblNm,
                    "saveMode": saveMode
                }
            },
            {
                "module": module,
                "stgNm": stgNm,
                "taskNm": taskNm,
                "paramNm": "table",
                "paramVal": tblNm,
                "effStatus": effStatus,
                "effDt": effDt2,
                "taskSeq": str(taskSeq)
            }
            ],
            "logClltFlg": logClltFlg,
            "statClltFlg": statClltFlg,
            "cacheFlg": cacheFlg,
            "persistType": persistType
        }
        return j

    def getCurrentTime(self, withZero: bool):
        date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        if withZero:
            return date + " 00:00:00.0"
        else:
            return date

    def setMainInfo(self, module, procNm, subProcNm, description, effStatus, createdBy,effDt="",projectNm="REGINSIGHT_APAC",legalEntity="CitiGroup",formNm="NA",projectid=0,sheduleNm="NA",source="ETL",contractTypeId="1"):
        if(effDt == ""):
            effDt = self.getCurrentTime(False)

        return {
            "module": module,
            "formNm": formNm,
            "sheduleNm": sheduleNm,
            "projectid": projectid,
            "procNm": procNm,
            "subProcNm": subProcNm,
            "source": source,
            "description": description,
            "contractTypeId": contractTypeId,
            "legalEntity": legalEntity,
            "projectNm": projectNm,
            "effDt": effDt,
            "effStatus": effStatus,
            "createdBy": createdBy,
            "processStages": []
        }
