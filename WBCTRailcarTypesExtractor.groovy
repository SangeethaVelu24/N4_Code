

import com.navis.argo.business.atoms.LogicalEntityEnum
import com.navis.carina.integrationservice.business.IntegrationService
import com.navis.external.framework.AbstractExtensionCallback
import groovy.sql.Sql
import org.apache.log4j.Logger
import wslite.json.JSONObject

class WBCTRailcarTypesExtractor extends AbstractExtensionCallback {
    def util = getLibrary("WBCTExtractorUtilLib")
    private static final Logger logger = Logger.getLogger(this.class)

    @Override
    void execute() {
        IntegrationService integrationService = null
        Sql sourceConn = util.establishDbConnection()
        sourceConn.eachRow(RTYPE_SQL) {
            row ->
                LinkedHashMap rcarTypeMap = generateRctypeMap(row)
                if(rcarTypeMap != null){
                    JSONObject jsonObject = new JSONObject(rcarTypeMap)
                    //  logger.warn("jsonObject "+jsonObject.toString())
                    util.logRequestToInterfaceMessage(LogicalEntityEnum.RO, integrationService, jsonObject.toString(), (String) rcarTypeMap.get("ID"))
                }
        }

    }

    def generateRctypeMap(rcarType){
        def rcarTypeMap = [:]
        rcarTypeMap.put("ID", rcarType.id)
        rcarTypeMap.put("NAME", rcarType.NAME)
        rcarTypeMap.put("DESCRIPTION", rcarType.DESCRIPTION)
        rcarTypeMap.put("STATUS", rcarType.STATUS)
        rcarTypeMap.put("CAR_TYPE", rcarType.CAR_TYPE)
        rcarTypeMap.put("MAX_20S", rcarType.MAX_20S)
        rcarTypeMap.put("MAX_TIER", rcarType.MAX_TIER)
        rcarTypeMap.put("FLOOR_HEIGHT", rcarType.FLOOR_HEIGHT)
        rcarTypeMap.put("HEIGHT_UNIT", rcarType.HEIGHT_UNIT)
        rcarTypeMap.put("FLOOR_LENGTH", rcarType.FLOOR_LENGTH)
        rcarTypeMap.put("LENGTH_UNIT", rcarType.LENGTH_UNIT)
        rcarTypeMap.put("TARE_WEIGHT", rcarType.TARE_WEIGHT)
        rcarTypeMap.put("TARE_UNIT", rcarType.TARE_UNIT)
        rcarTypeMap.put("SAFE_WEIGHT", rcarType.SAFE_WEIGHT)
        rcarTypeMap.put("SAFE_UNIT", rcarType.SAFE_UNIT)
        rcarTypeMap.put("NUM_PLATFORMS", rcarType.NUM_PLATFORMS)
        rcarTypeMap.put("LOWER20", rcarType.LOWER20)
        rcarTypeMap.put("UPPER20", rcarType.UPPER20)
        rcarTypeMap.put("LOWER40", rcarType.LOWER40)
        rcarTypeMap.put("UPPER40", rcarType.UPPER40)
        rcarTypeMap.put("LOWER45", rcarType.LOWER45)
        rcarTypeMap.put("UPPER45", rcarType.UPPER45)
        rcarTypeMap.put("LOWER48", rcarType.LOWER48)
        rcarTypeMap.put("UPPER48", rcarType.UPPER48)
        rcarTypeMap.put("HIGH_SIDE", rcarType.HIGH_SIDE)
        rcarTypeMap.put("CREATED", rcarType.CREATED)
        rcarTypeMap.put("CREATOR", rcarType.CREATOR)
        rcarTypeMap.put("CHANGED", rcarType.CHANGED)
        rcarTypeMap.put("CHANGER", rcarType.CHANGER)
        return rcarTypeMap
    }

    String RTYPE_SQL = """
		SELECT ID, NAME, STATUS,
        DESCRIPTION,
        CAR_TYPE, MAX_20S, MAX_TIER,
        FLOOR_HEIGHT , HEIGHT_UNIT,
        FLOOR_LENGTH, LENGTH_UNIT,
        TARE_WEIGHT, TARE_UNIT,
        SAFE_WEIGHT, SAFE_UNIT,
        HIGH_SIDE, CREATED, CREATOR,
        CHANGED, CHANGER, NUM_PLATFORMS, LOWER20, UPPER20, LOWER40, UPPER40, LOWER45, UPPER45, LOWER48, UPPER48 FROM (
    SELECT ID, NAME, STATUS,
        DESCRIPTION,
        CAR_TYPE, MAX_20S, MAX_TIER,
        FLOOR_HEIGHT , HEIGHT_UNIT,
        FLOOR_LENGTH, LENGTH_UNIT,
        TARE_WEIGHT, TARE_UNIT,
        SAFE_WEIGHT, SAFE_UNIT,
        HIGH_SIDE, CREATED, CREATOR,
        CHANGED, CHANGER, NUM_PLATFORMS, LOWER20, UPPER20, LOWER40, UPPER40, LOWER45, UPPER45, LOWER48, UPPER48,
           ROW_NUMBER() OVER(PARTITION BY ID ORDER BY ID) AS rn
    FROM DM_RAILCARTYPES
) WHERE rn = 1
		  """;
    // CUBIC_CAPACITY, CUBIC_UNIT,
    //        TEU_CAPACITY, VCG,
    //        VCG_UNIT, NUM_PLATFORMS,
}
