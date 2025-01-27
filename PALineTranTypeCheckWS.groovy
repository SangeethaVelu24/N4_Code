import com.navis.argo.ArgoRefField
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.EquipClassEnum
import com.navis.argo.business.atoms.EquipIsoGroupEnum
import com.navis.argo.business.atoms.EquipNominalHeightEnum
import com.navis.argo.business.atoms.EquipNominalLengthEnum
import com.navis.argo.business.model.GeneralReference
import com.navis.argo.business.reference.EquipType
import com.navis.argo.business.reference.LineOperator
import com.navis.external.argo.AbstractGroovyWSCodeExtension
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import groovy.json.JsonOutput
import org.apache.log4j.Logger

import java.text.SimpleDateFormat

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 03-Sep-2024
*
*  Requirements: If the line is restricted from dropping empties, the response should return 'true' else 'false'
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: PALineTranTypeCheckWS
*     Code Extension Type: GROOVY_WS_CODE_EXTENSION
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*  1.      13-09-2024     Sangeetha Velu  WBCT-212    Included the size type validation
*/

class PALineTranTypeCheckWS extends AbstractGroovyWSCodeExtension {
    @Override
    String execute(Map<String, String> inParameters) {
        LOGGER.warn("PALineTranTypeCheckWS executing...")
        String lineId = inParameters.get(LINE_ID)
        String tranType = inParameters.get(TRAN_TYPE)
        String sizeType = inParameters.get(SIZE_TYPE)
        List<Map<String, Object>> emptyRestrictedArray = new ArrayList<Map<String, Object>>();
        Map<String, Object> resultMapping = new HashMap<String, Object>();
        String[] tranTypeArray = new String[3]
        tranTypeArray[0] = EMPTY_IN
        tranTypeArray[1] = EMPTY_OUT
        tranTypeArray[2] = FULL_IN
        if (lineId != null && tranType != null && sizeType != null) {
            LineOperator lineOp = LineOperator.findLineOperatorById(lineId)
            if (lineOp != null && tranTypeArray.contains(tranType)) {
                resultMapping["RESULT"] = validateSizeType(sizeType, lineOp.getBzuId(), tranType)
                emptyRestrictedArray.add(resultMapping)
            }
        }
        return JsonOutput.toJson(emptyRestrictedArray);
    }


    boolean validateSizeType(String sizeType, String lineOp, String tranType) {

        Set<String> equipList = new HashSet<>()

        if (ArgoUtils.isNotEmpty(lineOp) && ArgoUtils.isNotEmpty(tranType)) {
            if (ArgoUtils.isNotEmpty(sizeType)) {
                if (sizeType.length() == 6) {
                    String ctrLength = sizeType.substring(0, 2)
                    String ctrISOGroup = sizeType.substring(2, 4)
                    String ctrHeight = sizeType.substring(sizeType.length() - 2, sizeType.length())

                    if (ctrLength != null && ctrHeight != null && ctrISOGroup != null) {
                        EquipIsoGroupEnum isoGroupEnum = EquipIsoGroupEnum.getEnum(ctrISOGroup)
                        EquipNominalHeightEnum heightEnum = EquipNominalHeightEnum.getEnum(NOM.concat(ctrHeight))
                        EquipNominalLengthEnum lengthEnum = EquipNominalLengthEnum.getEnum(NOM.concat(ctrLength))

                        if (isoGroupEnum != null && heightEnum != null && lengthEnum != null) {
                            DomainQuery dq = QueryUtils.createDomainQuery("EquipType")
                                    .addDqPredicate(PredicateFactory.eq(ArgoRefField.EQTYP_NOMINAL_HEIGHT, heightEnum))
                                    .addDqPredicate(PredicateFactory.eq(ArgoRefField.EQTYP_ISO_GROUP, isoGroupEnum))
                                    .addDqPredicate(PredicateFactory.eq(ArgoRefField.EQTYP_NOMINAL_LENGTH, lengthEnum))
                                    .addDqPredicate(PredicateFactory.eq(ArgoRefField.EQTYP_CLASS, EquipClassEnum.CONTAINER))
                            List<EquipType> equipTypeList = (List<EquipType>) HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
                            if (equipTypeList != null && equipTypeList.size() > 0) {

                                for (EquipType equipType : equipTypeList) {
                                    equipList.add(equipType.getEqtypArchetype().getEqtypId())
                                }
                                return isLockOutLogicApplicable(lineOp, equipList, tranType)
                            }
                        }
                    }
                }
            } else {
                return isLockOutLogicApplicable(lineOp, null, tranType)
            }
        }
        return false
    }

    private boolean isLockOutLogicApplicable(String lineOp, Set<String> equipList, String tranType) {
        if (ArgoUtils.isNotEmpty(lineOp) && ArgoUtils.isNotEmpty(tranType)) {
            if (equipList != null && equipList.size() > 0) {
                LOGGER.warn("equip list " + equipList)
                List<GeneralReference> genRefList = ( List<GeneralReference>)GeneralReference.findAllEntriesById(EMPTY_SHUTOUTS, tranType, lineOp)
                if (genRefList!=null && genRefList.size()>0){
                    for (GeneralReference genRef : genRefList){
                        if (genRef != null) {
                            if (equipList != null && equipList.size() > 0) {
                                if (equipList != null && equipList.contains(genRef.getRefId3())) {
                                    return validateGenRefValues(genRef)
                                }
                            }
                        }
                    }
                }
            } else {
                GeneralReference genRef = GeneralReference.findUniqueEntryById(EMPTY_SHUTOUTS, tranType, lineOp, null)
                LOGGER.warn("gen ref in else " + genRef)
                if (genRef != null) {
                    return validateGenRefValues(genRef)
                } else {
                    GeneralReference generalRef = GeneralReference.findUniqueEntryById(EMPTY_SHUTOUTS, EMPTY_SHUTOUTS_ALL, lineOp)
                    LOGGER.warn("general ref inner else " + generalRef)
                    if (generalRef != null) {
                        return validateGenRefValues(generalRef)
                    }
                }
            }
        }
        return false
    }

    private boolean validateGenRefValues(GeneralReference genRef) {
        if (genRef != null) {
            boolean isRefValue1Valid = ArgoUtils.isNotEmpty(genRef.getRefValue1()) && YES.equalsIgnoreCase(genRef.getRefValue1())
            if (isRefValue1Valid) {
                if (ArgoUtils.isNotEmpty(genRef.getRefValue2())) {
                    Date genRefDate = sdf.parse(genRef.getRefValue2())
                    if (genRefDate >= ArgoUtils.timeNow()) {
                        return true
                    }
                } else {
                    return true
                }
            }
        }
        return false
    }

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private static final String NOM = "NOM"
    private static final String EMPTY_SHUTOUTS = "EMPTY_SHUTOUTS"
    private static final String EMPTY_SHUTOUTS_ALL = "ALL"
    private static final String LINE_ID = "LINE_ID"
    private static final String TRAN_TYPE = "TRAN_TYPE"
    private static final String SIZE_TYPE = "SIZE_TYPE"
    private static final String EMPTY_IN = "EMPTY_IN"
    private static final String EMPTY_OUT = "EMPTY_OUT"
    private static final String FULL_IN = "FULL_IN"
    private static final String YES = "YES"
    private static final Logger LOGGER = Logger.getLogger(PALineTranTypeCheckWS.class)
}
