from pharmaPendium.searchService import SearchService
from pharmaPendium.sourceCfg import Source
import json

AERSService = SearchService(Source.SAFETY_AERS)


def getAERSQueryConditions():
    initialCond = {
        "speciesBoolean": {
            "initial": [{"andGroup": ["IBHRmo__7T4"]}]
        }
    }

    # get filter serious
    validOnes, exceedOnes = AERSService.getFilterSeries(initialCond)
    # 直接处理 exceedOnes，下面查询Efforts
    validQueryCondList = []
    with open('pharmaPendium/safetyData/AERS_qcond_drug', 'wb') as f:
        for exceedSample in exceedOnes:
            q1 = initialCond.copy()
            q1['serious'] = {'narrowing': [exceedSample['name']]}
            validOnes1, exceedOnes1 = AERSService.lookupFilters(q1, 'Drugs')
            for validSample1 in validOnes1:
                validSample1['serious'] = exceedSample['name']
            if len(validOnes1) > 0:
                print(f'valid query condition count:{len(validOnes1)}')
                lines = [f'{json.dumps(cond)}\n'.encode() for cond in validOnes1]
                f.writelines(lines)
