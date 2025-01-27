

import com.navis.argo.ArgoIntegrationEntity
import com.navis.argo.ArgoIntegrationField
import com.navis.argo.ContextHelper
import com.navis.argo.business.atoms.LogicalEntityEnum
import com.navis.argo.business.integration.IntegrationServiceMessage
import com.navis.external.framework.AbstractExtensionCallback
import com.navis.framework.business.atoms.LifeCycleStateEnum
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.DateUtil
import com.navis.rail.business.atoms.FlatCarTypeEnum
import com.navis.rail.business.entity.RailcarDetails
import com.navis.rail.business.entity.RailcarPlatformDetails
import com.navis.rail.business.entity.RailcarType
import com.navis.rail.business.entity.RailcarTypePlatform
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger
import wslite.json.JSONObject

class WBCTRailcarTypesMigrator extends AbstractExtensionCallback {
    StringBuilder errorMessage = new StringBuilder()

    void execute() {
        LOGGER.setLevel(Level.DEBUG)
        // LOGGER.debug("WBCTRailcarTypesMigrator started !!!!!!!!!")
        List<IntegrationServiceMessage> ismList = getMessagesToProcess()

        for (IntegrationServiceMessage ism : ismList) {
            errorMessage = new StringBuilder()
            String jsonPayload = ism.getIsmMessagePayload()
            if (StringUtils.isNotEmpty(jsonPayload)) {
                JSONObject jsonObj = new JSONObject(jsonPayload);
                def rcarType = processRcarType(jsonObj)
                if (getMessageCollector().hasError() && getMessageCollector().getMessageCount() > 0) {
                    errorMessage.append(getMessageCollector().toCompactString()).append("::")
                }
                if (errorMessage.toString().length() > 0) {
                    ism.setIsmUserString4(errorMessage.toString())
                } else if (rcarType != null) {
                    ism.setIsmUserString3("true")
                }
                HibernateApi.getInstance().save(ism)
            } else {
                LOGGER.warn("No JSON Payload for " + ism.getIsmUserString1())
            }
        }
    }

    def processRcarType(jsonObj) {
        String id = jsonObj.getOrDefault("ID", null)
        if (StringUtils.isEmpty(id)) {
            errorMessage.append("ID is required " + id).append("::")
            return null
        }

        RailcarType rcarType = RailcarType.findRailcarType(id);
        if (rcarType == null) {
            rcarType = RailcarType.createRailcarType(id);
        }
        RailcarDetails rcDetails;
        Boolean isNewCarDetails = Boolean.FALSE;

        String name = jsonObj.getOrDefault("NAME", null)
        rcarType.setRcartypName(name)
        rcDetails = rcarType.getRcartypRailcarDetails();
        if (rcDetails == null) {
            rcDetails = new RailcarDetails();
            isNewCarDetails = Boolean.TRUE;
        }
        String status = jsonObj.getOrDefault("STATUS", null)
        String car_type = jsonObj.getOrDefault("CAR_TYPE", null)
        String max_20s = jsonObj.getOrDefault("MAX_20S", null)
        String max_tier = jsonObj.getOrDefault("MAX_TIER", null)
        String floor_height = jsonObj.getOrDefault("FLOOR_HEIGHT", null)
        String height_unit = jsonObj.getOrDefault("HEIGHT_UNIT", null)
        String floor_length = jsonObj.getOrDefault("FLOOR_LENGTH", null)
        String length_unit = jsonObj.getOrDefault("LENGTH_UNIT", null)
        String tare_weight = jsonObj.getOrDefault("TARE_WEIGHT", null)
        String tare_unit = jsonObj.getOrDefault("TARE_UNIT", null)
        String safe_weight = jsonObj.getOrDefault("SAFE_WEIGHT", null)
        String safe_unit = jsonObj.getOrDefault("SAFE_UNIT", null)
        String num_platforms = jsonObj.getOrDefault("NUM_PLATFORMS", null)
        String lower20 = jsonObj.getOrDefault("LOWER20", null)
        boolean isLower20 = "Y".equalsIgnoreCase(lower20)
        String lower40 = jsonObj.getOrDefault("LOWER40", null)
        boolean isLower40 = "Y".equalsIgnoreCase(lower40)
        String lower45 = jsonObj.getOrDefault("LOWER45", null)
        boolean isLower45 = "Y".equalsIgnoreCase(lower45)
        String lower48 = jsonObj.getOrDefault("LOWER48", null)
        boolean isLower48 = "Y".equalsIgnoreCase(lower48)
        String upper20 = jsonObj.getOrDefault("UPPER20", null)
        boolean isUpper20 = "Y".equalsIgnoreCase(upper20)
        String upper40 = jsonObj.getOrDefault("UPPER40", null)
        boolean isUpper40 = "Y".equalsIgnoreCase(upper40)
        String upper45 = jsonObj.getOrDefault("UPPER45", null)
        boolean isUpper45 = "Y".equalsIgnoreCase(upper45)
        String upper48 = jsonObj.getOrDefault("UPPER48", null)
        boolean isUpper48 = "Y".equalsIgnoreCase(upper48)


        String created = jsonObj.getOrDefault("CREATED", null)
        Date ct = DateUtil.getTodaysDate(ContextHelper.getThreadUserContext().getTimeZone())
        if (created) {
            ct = DateUtil.dateStringToDate(DateUtil.parseStringToDate(created, ContextHelper.getThreadUserContext()).format('yyyy-MM-dd HH:mm:ss'))
        }
        rcarType.rcartypCreated = ct;

        String changed = jsonObj.getOrDefault("CHANGED", null)
        Date changedDate = DateUtil.getTodaysDate(ContextHelper.getThreadUserContext().getTimeZone())
        if (changed) {
            changedDate = DateUtil.dateStringToDate(DateUtil.parseStringToDate(changed, ContextHelper.getThreadUserContext()).format('yyyy-MM-dd HH:mm:ss'))
        }
        rcarType.rcartypChanged = changedDate;

        String creator = jsonObj.getOrDefault("CREATOR", null)
        rcarType.rcartypCreator = creator
        String changer = jsonObj.getOrDefault("CHANGER", null)
        rcarType.rcartypChanger = changer
        rcDetails.setRcdFlatCarType(getFlatCarType(car_type));
        rcDetails.setRcdIsHighSide(Boolean.FALSE); // included as there is no information on high side in M21
        rcDetails.rcdMaxTiersPerPlatform = (max_tier ? Long.parseLong(max_tier) : 0);
        rcDetails.rcdFloorHeightCm = convertLengthToCM(floor_height, height_unit);
        rcDetails.rcdLengthCm = convertLengthToCM(floor_length, length_unit);
        rcDetails.rcdMax20sPerPlatform = (max_20s ? Long.parseLong(max_20s) : 0);
        rcDetails.rcdTareWeightKg = convertWeightToKG(tare_weight, tare_unit);
        rcDetails.rcdSafeWeightKg = convertWeightToKG(safe_weight, safe_unit);
        rcarType.rcartypLifeCycleState = ((status == 'X') ? LifeCycleStateEnum.OBSOLETE : LifeCycleStateEnum.ACTIVE);
        rcarType.setRcartypRailcarDetails(rcDetails);
        HibernateApi.getInstance().save(rcarType);

        if (!StringUtils.isEmpty(num_platforms)) {
            int platformCount = Integer.valueOf(num_platforms)
            for (int i = 1; i <= platformCount; i++) {
                processRailCarPlatform(rcarType, i, platformCount, isLower20, isLower40, isLower45, isLower48, isUpper20, isUpper40, isUpper45, isUpper48, ct, changedDate)
            }
        }
        return  rcarType
    }

    private FlatCarTypeEnum getFlatCarType(String inCarType) {
        if (inCarType == null)
            return FlatCarTypeEnum.COFC; //return null is changed to COFC

        switch (inCarType) {
            case 'FLAT': return FlatCarTypeEnum.COFC;
            case 'SPINE': return FlatCarTypeEnum.CONV;
            case 'STACK': return FlatCarTypeEnum.TOFC;
            case 'BOX': return FlatCarTypeEnum.TOFC;
            case 'DS': return FlatCarTypeEnum.COFC
            case 'SP': return FlatCarTypeEnum.CONV
        }
        return FlatCarTypeEnum.TOFC;
    }

    private Double convertLengthToCM(String length, String unit) {
        if (length == null || length.isEmpty() || length == '')
            return 0;
        Double w = Double.valueOf(length);
        switch (unit) {
            case 'CM': return w;
            case 'IN': return (w * 2.54);
            case 'FT': return (w * 30.48);
            case 'M': return (w * 100);
            case 'KM': return (w * 100000);
            case 'TENTH_OF_INCH': return (w * 0.254)
            default: return w; // throw new Exception("Not able to map length unit " + unit);
        }
    }

    private Double convertWeightToKG(String grossWeight, String unit) {
        if (grossWeight == null || grossWeight.isEmpty() || grossWeight == '')
            return null;
        Double w = Double.valueOf(grossWeight);
        switch (unit) {
            case 'KG': return w;
            case 'KT': return (w * 1000000).round();
            case 'LB': return (w * 0.4535923699997481);
            case 'MT': return (w * 1000).round();
            case 'LT': return w;
        }
        return w;
    }

    private void processRailCarPlatform(RailcarType inRcarType, int inPlatformSeq, int wellQty, Boolean lower20, Boolean lower40, Boolean lower45, Boolean lower48, Boolean upper20, Boolean upper40, Boolean upper45, Boolean upper48, Date createdDate, Date changedDate) {
        LOGGER.debug("Inside processRailCarPlatform")
        String label = null;
        int wellQuantity = wellQty
        int pfSeq = inPlatformSeq
        if(wellQuantity == 5){
            if(pfSeq == 1){
                label = SEQ_A
            }else if(pfSeq == 2){
                label = SEQ_E
            }else if(pfSeq == 3){
                label = SEQ_D
            }else if(pfSeq == 4){
                label = SEQ_C
            }else if(pfSeq == 5){
                label = SEQ_B
            }
        }else if(wellQuantity == 3){
            if(pfSeq == 1){
                label = SEQ_A
            }else if(pfSeq == 2){
                label = SEQ_C
            }else if(pfSeq == 3){
                label = SEQ_B
            }
        }else if(wellQuantity == 1){
            if(pfSeq == 1){
                label = SEQ_A
            }
        }

        if(wellQuantity == 1 || wellQuantity == 3 || wellQuantity == 5) {
            RailcarTypePlatform platform =  RailcarTypePlatform.findOrCreateRailcarTypePlatform(inRcarType, label, (Long)pfSeq)
            RailcarPlatformDetails rcpDetails = platform.getRcartyplfRailcarPlatformDetails()
            if(rcpDetails == null){
                rcpDetails = new RailcarPlatformDetails();
            }

            rcpDetails.setPlfdSequence((Long)pfSeq);
            rcpDetails.setPlfdLabel(label);
            rcpDetails.setPlfdIsLower20(lower20);
            rcpDetails.setPlfdIsLower40(lower40);
            rcpDetails.setPlfdIsLower45(lower45);
            rcpDetails.setPlfdIsLower48(lower48);
            rcpDetails.setPlfdIsUpper20(upper20);
            rcpDetails.setPlfdIsUpper40(upper40);
            rcpDetails.setPlfdIsUpper45(upper45);
            rcpDetails.setPlfdIsUpper48(upper48);
            platform.setRcartyplfCreator('MIGRATION')
            platform.setRcartyplfCreated(createdDate)
            platform.setRcartyplfChanger('MIGRATION')
            platform.setRcartyplfChanged(changedDate)
            platform.updateRcartyplfRailcarPlatformDetails(rcpDetails);
            HibernateApi.getInstance().save(platform);

        }

    }

    List<IntegrationServiceMessage> getMessagesToProcess() {
        DomainQuery domainQuery = QueryUtils.createDomainQuery(ArgoIntegrationEntity.INTEGRATION_SERVICE_MESSAGE)
        domainQuery.addDqPredicate(PredicateFactory.eq(ArgoIntegrationField.ISM_ENTITY_CLASS, LogicalEntityEnum.RO));
        domainQuery.addDqPredicate(PredicateFactory.eq(ArgoIntegrationField.ISM_USER_STRING3, "false"))
        domainQuery.addDqOrdering(Ordering.asc(ArgoIntegrationField.ISM_CREATED));

        return HibernateApi.getInstance().findEntitiesByDomainQuery(domainQuery)
    }

    private static final String SEQ_A = 'A'
    private static final String SEQ_B = 'B'
    private static final String SEQ_C = 'C'
    private static final String SEQ_D = 'D'
    private static final String SEQ_E = 'E'
    private static final Logger LOGGER = Logger.getLogger(this.class)
}

