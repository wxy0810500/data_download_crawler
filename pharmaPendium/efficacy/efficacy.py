from pharmaPendium.searchService import SearchService
from pharmaPendium.sourceCfg import Source, SOURCE_CFG
import pandas as pd
import time
import json

CONFIG = SOURCE_CFG[Source.EFFICACY]
searchService = SearchService(Source.EFFICACY)
searchBody = {"isPreclinical": False, "specieIDs": {"initial": ["IBHRmo__7T4"]},
                  "sortColumns": [{"column": "drug", "isAscending": True},
                                  {"column": "standardIndications", "isAscending": True},
                                  {"column": "endpointMid", "isAscending": True}],
                  "displayColumns": ["drug",
                                     "smiles",
                                     "studyNumber",
                                     "phase",
                                     "monoCombination",
                                     "studyDesign",
                                     "specie",
                                     "sex",
                                     "age",
                                     "standardIndications",
                                     "indication",
                                     "pathogen",
                                     "route",
                                     "dose",
                                     "doseFrequency",
                                     "treatment",
                                     "timePoint",
                                     "placebo",
                                     "baseline",
                                     "sampleSize",
                                     "comparativeGroup",
                                     "endpointType",
                                     "endpointMid",
                                     "endpointLow",
                                     "endpoint",
                                     "valueOriginal",
                                     "unitOriginal",
                                     "pValue",
                                     "population",
                                     "experimentalDetail",
                                     "dataProvider",
                                     "document",
                                     "documentYear",
                                     "id"]}


def exportClinical(firstRow=0, totalCount=0, multiProcess: bool = False):

    if multiProcess:
        searchService.searchToCsv_multiprocess(searchBody, 'pharmaPendium/data/efficacy', 'clinical', firstRow)
    else:
        searchService.searchToCsv(searchBody, 'pharmaPendium/data/efficacy', 'clinical', firstRow, totalCount)


def exportPreClinical(firstRow=0, totalCount=0, multiProcess: bool = False):
    # other preclinical
    searchBody['isPreclinical'] = True
    searchBody["specieIDs"] = {"initial": ["ALOvP_GaRv9"]}
    if multiProcess:
        searchService.searchToCsv_multiprocess(searchBody, 'pharmaPendium/data/efficacy', 'preClinical', firstRow)
    else:
        searchService.searchToCsv(searchBody, 'pharmaPendium/data/efficacy', 'preClinical', firstRow, totalCount)


def exportConditions():
    """
     下载逻辑 
     Efficacy clinical data(size 3.15 million)
        1.initial: species —> only human --> getting clinical data
        2.filter: placebo --> years (<5w checked)
                  No placebo -->  years (<5w checked) -->Data provider
        download:Clinical Data
    """
    # initial:species

    # df = pd.read_csv(f'{taxonomyTreeFilePath}/{source}/{source.title()}-Species.csv')
    validQueryCondList = []
    queryCond = {'isPreclinical': False,
                 "specieIDs": {"initial": ["IBHRmo__7T4"]}}
    # q1: dict = queryCond.copy()
    # validOnes1, exceedOnes1 = exportService.lookupFilters(queryCond, "Species")
    # if len(validOnes1) > 0:
    #     for validCond1 in validOnes1:
    #         q1["specieIDs"] = {"initial": [data['id'] for data in validCond1['dataGroup']]}
    #         validQueryCondList.append({'queryCond': q1.copy(), 'count': validCond1["count"]})
    # if len(exceedOnes1) > 0:
    #     for exceedCond1 in exceedOnes1:
    #         q1["specieIDs"] = {"initial": [exceedCond1['id']]}
    q2: dict = queryCond.copy()
    # filter 1 placebo
    validOnes2, exceedOnes2 = searchService.getFilterPlacebo(q2)
    if len(validOnes2) > 0:
        for validCond2 in validOnes2:
            q2["placebo"] = {"narrowing": [validCond2['name']]}
            validQueryCondList.append({'queryCond': q2.copy(), 'count': validCond2["count"]})
    if len(exceedOnes2) > 0:
        for exceedCond2 in exceedOnes2:
            q2["placebo"] = {"narrowing": [exceedCond2['name']]}
            q3 = q2.copy()
            # filter 2 ：years
            validOnes3, exceedOnes3 = searchService.getFilterYears(q3)
            if len(validOnes3) > 0:
                for validCond3 in validOnes3:
                    q3["years"] = {"narrowing": validCond3["data"]}
                    validQueryCondList.append({'queryCond': q3.copy(), 'count': validCond3["count"]})
            if len(exceedOnes3) > 0:
                for exceedCond3 in exceedOnes3:
                    q3["years"] = {"narrowing": [exceedCond3["name"]]}
                    q4 = q3.copy()
                    # filter3: data providers
                    validOnes4, exceedOnes4 = searchService.getFilterDataProvider(q4)
                    if len(validOnes4) > 0:
                        for validCond4 in validOnes4:
                            q4["dataProviders"] = {"narrowing": validCond4['data']}
                            validQueryCondList.append({'queryCond': q4.copy(), 'count': validCond4["count"]})
                    if len(exceedOnes4) > 0:
                        print(f"{q3} ==== {exceedOnes4}")
    if len(validQueryCondList) > 0:
        print(f'valid query condition count:{len(validQueryCondList)}')
        with open('pharmaPendium/data/efficacy/qcond/efficacy-qcond-clinical', 'wb') as f:
            lines = [f'{json.dumps(cond)}\n'.encode() for cond in validQueryCondList]
            f.writelines(lines)