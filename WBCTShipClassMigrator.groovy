package WBCT

import com.navis.argo.ArgoIntegrationEntity
import com.navis.argo.ArgoIntegrationField
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.LogicalEntityEnum
import com.navis.argo.business.integration.IntegrationServiceMessage
import com.navis.external.framework.AbstractExtensionCallback
import com.navis.framework.business.atoms.LifeCycleStateEnum
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.vessel.business.atoms.VesselTypeEnum
import com.navis.vessel.business.operation.VesselClass
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import wslite.json.JSONObject

class WBCTShipClassMigrator extends AbstractExtensionCallback {
    StringBuilder errorMessage = new StringBuilder()

    void execute() {
        List<IntegrationServiceMessage> ismList = getMessagesToProcess()

        for (IntegrationServiceMessage ism : ismList) {
            errorMessage = new StringBuilder()

            String jsonPayload = ism.getIsmMessagePayload()
            if (StringUtils.isNotEmpty(jsonPayload)) {
                JSONObject jsonObj = new JSONObject(jsonPayload);
                VesselClass vesselClass = processVslClass(jsonObj)
                if (getMessageCollector().hasError() && getMessageCollector().getMessageCount() > 0) {
                    errorMessage.append(getMessageCollector().toCompactString()).append("::")
                }
                if (errorMessage.toString().length() > 0) {
                    ism.setIsmUserString4(errorMessage.toString())
                } else if (vesselClass != null) {
                    ism.setIsmUserString3("true")
                }

            } else {
                LOGGER.warn("No JSON Payload for " + ism.getIsmUserString1())
            }
        }
    }

    def processVslClass(jsonObj) {
        String id = jsonObj.getOrDefault("id", null)
        String name = jsonObj.getOrDefault("name", null)
        String ship_type = jsonObj.getOrDefault("ship_type", null)
        VesselTypeEnum vslType;

        if (StringUtils.isEmpty(id)) {
            errorMessage.append("ID is required " + id).append("::")
            return null
        }

        if (StringUtils.isEmpty(name)) {
            /*errorMessage.append("Name is required " + name).append("::")
            return null*/
            name = id // name is null for 13 records in M21 db, hence assigning the id value to name
        }

        if (ship_type != null) {
            vslType = convertVesselType(ship_type)
        }

        VesselClass vesselClass = VesselClass.findVesselClassById(id)
        if (vesselClass == null) {
            vesselClass = VesselClass.createVesselClass(id, name, vslType)
            vesselClass.vesclassIsSelfSustaining = false
        }

        String loa = jsonObj.getOrDefault("loa", null)
        String loa_units = jsonObj.getOrDefault("loa_units", null)

        if (loa) {
            double loaValue = convertLoa(loa, loa_units)
            vesselClass.vesclassLoaCm = loaValue
        }


        String beam = jsonObj.getOrDefault("beam", null)
        String beam_units = jsonObj.getOrDefault("beam_units", null)

        if (beam) {
            double beamValue = convertBeam(beam, beam_units)
            vesselClass.vesclassBeamCm = beamValue
        }

        String bow_overhang = jsonObj.getOrDefault("bow_overhang", null)
        String bow_units = jsonObj.getOrDefault("bow_units", null)

        if (bow_overhang) {
            double bowVal = convertBow(bow_overhang, bow_units)
            vesselClass.vesclassBowOverhangCm = bowVal
        }

        String stern_overhang = jsonObj.getOrDefault("stern_overhang", null)
        String stern_units = jsonObj.getOrDefault("stern_units", null)

        if (stern_overhang) {
            double stern = convertBow(stern_overhang, stern_units)
            vesselClass.vesclassSternOverhangCm = stern
        }

        String bridge_to_bow = jsonObj.getOrDefault("bridge_to_bow", null)
        String bridge_to_bow_units = jsonObj.getOrDefault("bridge_to_bow_units", null)

        if (bridge_to_bow) {
            double bBow = convertBow(bridge_to_bow, bridge_to_bow_units)
            vesselClass.vesclassSternOverhangCm = bBow
        }

        String bays_aft = jsonObj.getOrDefault("bays_aft", null)
        Long bayAft = (bays_aft ? Long.parseLong(bays_aft) : null);
        if (bayAft) {
            vesselClass.setVesclassBaysAft(bayAft);
        }

        String bays_forward = jsonObj.getOrDefault("bays_forward", null)
        Long bayFwd = (bays_forward ? Long.parseLong(bays_forward) : null);
        if (bayFwd) {
            vesselClass.setVesclassBaysForward(bayFwd);
        }

        String self_sustaining = jsonObj.getOrDefault("self_sustaining", null)
        if (self_sustaining) {
            vesselClass.setVesclassIsSelfSustaining(Boolean.valueOf(self_sustaining))
        }

        String notes = jsonObj.getOrDefault("notes", null)
        vesselClass.vesclassNotes = notes;
        vesselClass.vesclassName = name;
        String active = jsonObj.getOrDefault("active", null)
        boolean isActive = active == 'X'
        vesselClass.setVesclassIsActive(isActive)

        vesselClass.setVesclassCreated(ArgoUtils.timeNow())
        vesselClass.setVesclassChanged(ArgoUtils.timeNow())
        String creator = jsonObj.getOrDefault("creator", null)
        vesselClass.vesclassCreator = creator

        String changer = jsonObj.getOrDefault("changer", null)
        vesselClass.vesclassChanger = changer
        vesselClass.vesclassLifeCycleState = LifeCycleStateEnum.ACTIVE;
        vesselClass.setVesclassVesselType(vslType);
        return vesselClass

    }

    List<IntegrationServiceMessage> getMessagesToProcess() {
        DomainQuery domainQuery = QueryUtils.createDomainQuery(ArgoIntegrationEntity.INTEGRATION_SERVICE_MESSAGE)
        domainQuery.addDqPredicate(PredicateFactory.eq(ArgoIntegrationField.ISM_ENTITY_CLASS, LogicalEntityEnum.CV));
        domainQuery.addDqPredicate(PredicateFactory.eq(ArgoIntegrationField.ISM_USER_STRING3, "false"))
        domainQuery.addDqOrdering(Ordering.asc(ArgoIntegrationField.ISM_CREATED));

        return HibernateApi.getInstance().findEntitiesByDomainQuery(domainQuery)
    }

    Double convertLoa(loa, units) {
        if (StringUtils.isEmpty(loa))
            return null;

        if (StringUtils.isEmpty(units)) {
            errorMessage.append("LOA Units is required if LOA has a value").append("::")
            return null
        }

        Double value = Double.parseDouble(loa);
        switch (units) {
            case 'M': return value * 100;
            case 'MT': return value * 100;
            case 'FT': return value * 100000;
        }
        throw new Exception("Unknown LOA Unit " + units);
    }

    private Double convertBeam(beam, units) {
        if (beam == '' || beam == null)
            return null

        if (units == '' || units == null)
            throw new Exception('BEAM Units is required if BEAM has a value')

        Double value = Double.parseDouble(beam);
        switch (units) {
            case 'M': return value * 100;
            case 'MT': return value * 100;
            case 'FT': return value * 100000;
        }
        throw new Exception("Unknown BEAM Unit " + units);
    }

    private Double convertBow(bow, units) {
        if (bow == '' || bow == null)
            return null

        if (units == '' || units == null)
            throw new Exception('BOW Units is required if BOW has a value')

        Double value = Double.parseDouble(bow)
        switch (units) {
            case 'M': return value * 100;
            case 'MT': return value * 100;
            case 'FT': return value * 100000;
        }
        throw new Exception("Unknown BOW Unit " + units)
    }

    private VesselTypeEnum convertVesselType(String expressShipType) {
        VesselTypeEnum vslType;
        switch (expressShipType) {
            case "BARGE":
                vslType = VesselTypeEnum.BARGE;
                break;
            case "BBLK":
            case "BULK":
                vslType = VesselTypeEnum.BBULK;
                break;
            case "COVE":
            case "LOLO":
            case "SPOP":
            case "MAIN":
                vslType = VesselTypeEnum.CELL;
                break;
            case "POPA":
                vslType = VesselTypeEnum.PSNGR;
                break;
            case "RORO":
                vslType = VesselTypeEnum.RORO;
                break;
            case "DRY":
            case "FEED":
            case "N/A":
            case "NOCEL":
            case "NONCE":
                vslType = VesselTypeEnum.UNKNOWN;
                break;
            default:
                vslType = VesselTypeEnum.CELL;// all other cases default to Container Ship Type
        }
        return vslType;
    }

    //private static final String ENTITY = 'Vessel'
    private static final Logger LOGGER = Logger.getLogger(this.class)
}
