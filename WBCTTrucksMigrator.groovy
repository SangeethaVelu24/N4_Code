

import com.navis.argo.ArgoIntegrationEntity
import com.navis.argo.ArgoIntegrationField
import com.navis.argo.business.atoms.LogicalEntityEnum
import com.navis.argo.business.integration.IntegrationServiceMessage
import com.navis.external.framework.AbstractExtensionCallback
import com.navis.framework.business.atoms.LifeCycleStateEnum
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.BizFailure
import com.navis.framework.util.BizViolation
import com.navis.road.business.atoms.TruckStatusEnum
import com.navis.road.business.model.Truck
import com.navis.road.business.model.TruckingCompany
import com.navis.road.business.util.RoadBizUtil
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger
import wslite.json.JSONObject

import java.text.SimpleDateFormat

//
class WBCTTrucksMigrator extends AbstractExtensionCallback {
    StringBuilder errorMessage = new StringBuilder()

    String execute() {
        LOGGER.setLevel(Level.DEBUG)
        LOGGER.debug("WBCTTrucksMigrator started execution")
        List<IntegrationServiceMessage> ismList = getMessagesToProcess()

        for (IntegrationServiceMessage ism : ismList) {
            errorMessage = new StringBuilder()

            String jsonPayload = ism.getIsmMessagePayload()
            if (StringUtils.isNotEmpty(jsonPayload)) {
                ism.setIsmUserString4(null)
                JSONObject jsonObj = new JSONObject(jsonPayload);
                Truck truck = null

                try {
                    truck = processTruckMap(jsonObj)

                } catch (Exception e) {
                    errorMessage.append("" + e.getMessage().take(100)).append("::")
                }

                if (getMessageCollector().hasError() && getMessageCollector().getMessageCount() > 0) {
                    errorMessage.append(getMessageCollector().toCompactString()).append("::")
                }
                if (errorMessage.toString().length() > 0) {
                    if (errorMessage.toString().length() < 255) {
                        ism.setIsmUserString4(errorMessage.toString())
                    } else {
                        ism.setIsmUserString4("Too many error messages")
                    }
                } else if (truck != null) {
                    ism.setIsmUserString3("true")
                }
                HibernateApi.getInstance().save(ism)
                RoadBizUtil.commit()

            } else {
                LOGGER.warn("No JSON Payload for " + ism.getIsmUserString1())
            }
        }
    }

    def processTruckMap(JSONObject jsonObj) {

        Double DEFAULT_TARE_WEIGHT_KG = 7938; //17,500 lb
        Double MIN_TARE_WEIGHT_KG = 5443; //12,000 lb
        String TRUCK_ID = jsonObj.getOrDefault("TRUCK_ID", null)
        String LICENSE_NBR = jsonObj.getOrDefault("LICENSE_NBR", null)
        String LICENSE_STATE = jsonObj.getOrDefault("LICENSE_STATE", null)
        Date EXPIRATION = jsonObj.getOrDefault("EXPIRATION", null) ? parseDate(jsonObj.getOrDefault("EXPIRATION", null)) : null;
        String AEI_TAG_ID = jsonObj.getOrDefault("AEI_TAG_ID", null)
        Double TARE_WEIGHT = jsonObj.getOrDefault("TARE_WEIGHT", null) ? Double.parseDouble((String) jsonObj.getOrDefault("TARE_WEIGHT", null)) : DEFAULT_TARE_WEIGHT_KG;
        if (TARE_WEIGHT < MIN_TARE_WEIGHT_KG)
            TARE_WEIGHT = DEFAULT_TARE_WEIGHT_KG;
        String TRUCKING_COMPANY = jsonObj.getOrDefault("TRUCKING_COMPANY", null)
        String BANNED = jsonObj.getOrDefault("BANNED", null)
        Date CREATED = jsonObj.getOrDefault("CREATED", null) ? parseDate(jsonObj.getOrDefault("CREATED", null)) : null;
        String CREATOR = jsonObj.getOrDefault("CREATOR", null)
        TruckingCompany truckCo = null;

        if ((LICENSE_NBR == null) || (LICENSE_NBR == "")) {
            errorMessage.append("License plate number is missing" + LICENSE_NBR).append("::")
            return null
        }

        if ((TRUCKING_COMPANY != null) && (TRUCKING_COMPANY != "")) {
            truckCo = TruckingCompany.findTruckingCompany(TRUCKING_COMPANY);
            //if (truckCo == null)
            //truckCo = importTruckingCompany(TRUCKING_COMPANY);
        }

        // try to find active truck
        Truck truck = Truck.findTruck(LICENSE_NBR, TRUCK_ID, truckCo, null, null, LifeCycleStateEnum.ACTIVE);

        // if not found, try to find obsolete truck
        if (truck == null)
            truck = Truck.findTruck(LICENSE_NBR, TRUCK_ID, truckCo, null, null, LifeCycleStateEnum.OBSOLETE);

        // if still not found, try to create
        if (truck == null)
            truck = Truck.create(LICENSE_NBR, TRUCK_ID, TARE_WEIGHT, null, null, truckCo);
        else if ((truck.truckId != TRUCK_ID) && (truck.truckLicenseNbr == LICENSE_NBR)) {
            errorMessage.append("Truck with the same license plate number already exists in N4" + LICENSE_NBR).append("::")
            return null
        }


        if (truck == null) {
            errorMessage.append("Truck creation failed")
        }


        try {
            truck.truckLicenseState = LICENSE_STATE;
            truck.truckLicenseExpiration = EXPIRATION;
            truck.truckTareWeight = TARE_WEIGHT;
            truck.truckTrkCo = truckCo;
            truck.truckStatus = BANNED == "Y" ? TruckStatusEnum.BANNED : TruckStatusEnum.OK;
            truck.lifeCycleState = LifeCycleStateEnum.ACTIVE;
            // truck.truckAeiTagId = AEI_TAG_ID
            truck.truckCreated = CREATED;
            truck.truckCreator = creatorOrChanger;
            truck.truckChanged = new java.util.Date();
            truck.truckChanger = creatorOrChanger
        } catch (BizViolation | BizFailure bv) {
            errorMessage.append(bv.getLocalizedMessage()).append("::")
            return null
        }

        HibernateApi.getInstance().save(truck);

        return truck;

    }


    private Date parseDate(inDate) {
        if (inDate == null || inDate == '' || inDate == 'null')
            return null

        return inDate ? new java.text.SimpleDateFormat('yyyyMMddHHmmss').parse(inDate) : ''
        //return inDate ? new java.text.SimpleDateFormat('yyyy-MM-dd HH mm ss').parse(inDate) : ''
        //return date ? new java.text.SimpleDateFormat('yyyy MM dd HH:mm:ss').parse(date) : ''
    }

    private Date getExpirationDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("dd-M-yyyy hh:mm:ss");
        String dateInString = "01-01-2029 00:00:00";
        return sdf.parse(dateInString);
    }


    List<IntegrationServiceMessage> getMessagesToProcess() {
        DomainQuery domainQuery = QueryUtils.createDomainQuery(ArgoIntegrationEntity.INTEGRATION_SERVICE_MESSAGE)
        domainQuery.addDqPredicate(PredicateFactory.eq(ArgoIntegrationField.ISM_ENTITY_CLASS, LogicalEntityEnum.LO));
        domainQuery.addDqPredicate(PredicateFactory.eq(ArgoIntegrationField.ISM_USER_STRING3, "false"))
        //domainQuery.addDqPredicate(PredicateFactory.eq(ArgoIntegrationField.ISM_USER_STRING1,"28274"))
        domainQuery.addDqOrdering(Ordering.asc(ArgoIntegrationField.ISM_CREATED));
        domainQuery.setDqMaxResults(100)
        return HibernateApi.getInstance().findEntitiesByDomainQuery(domainQuery)
    }

    private static final Logger LOGGER = Logger.getLogger(this.class)
    private static final creatorOrChanger = "Migration"
}