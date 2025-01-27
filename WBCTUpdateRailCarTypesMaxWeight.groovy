package WBCT


import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.rail.RailEntity
import com.navis.rail.RailField
import com.navis.rail.business.entity.RailcarDetails
import com.navis.rail.business.entity.RailcarPlatformDetails
import com.navis.rail.business.entity.RailcarType
import com.navis.rail.business.entity.RailcarTypePlatform

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date:
*
*  Requirements:
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name:
*     Code Extension Type:
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTUpdateRailCarTypesMaxWeight {

    String execute() {
        StringBuilder sb = new StringBuilder()
        Double safeWeight = 0.0

        Double maxWeight = 0.0
        DomainQuery dq = QueryUtils.createDomainQuery(RailEntity.RAILCAR_TYPE)
                .addDqPredicate(PredicateFactory.isNotNull(RailField.RCARTYP_ID))

        List<RailcarType> railcarTypeList = (List<RailcarType>) HibernateApi.getInstance().findEntitiesByDomainQuery(dq)

        if (railcarTypeList != null && railcarTypeList.size() > 0) {
            for (RailcarType railcarType : railcarTypeList) {

                // RailcarType railcarType = (RailcarType) railcarTypeList.get(0)
                sb.append("railCar type id " + railcarType.getRcartypId()).append("\n")

                RailcarDetails railcarDetails = railcarType.getRcartypRailcarDetails()
                sb.append("rail car details " + railcarDetails).append("\n")
                if (railcarDetails != null && railcarDetails.getRcdSafeWeightKg() != null) {
                    safeWeight = railcarDetails.getRcdSafeWeightKg()
                    sb.append("safe weight " + safeWeight).append("\n")
                    Set<RailcarTypePlatform> railcarTypePlatformSet = (Set<RailcarTypePlatform>) railcarType.getRcartypPlatforms()
                    sb.append("railcarTypePlatformSet " + railcarTypePlatformSet).append("\n")
                    if (railcarTypePlatformSet != null && railcarTypePlatformSet.size() > 0) {
                        int railCarPlatFormSize = railcarTypePlatformSet.size()
                        sb.append("railCarPlatFormSize " + railCarPlatFormSize).append("\n")
                        maxWeight = safeWeight / railCarPlatFormSize
                        sb.append("max weight " + maxWeight).append("\n")
                        for (RailcarTypePlatform railcarTypePlatform : railcarTypePlatformSet) {
                            RailcarPlatformDetails railcarPlatformDetails = railcarTypePlatform.rcartyplfRailcarPlatformDetails
                            sb.append("railcarPlatformDetails " + railcarPlatformDetails).append("\n")
                            if (railcarPlatformDetails != null) {
                                railcarPlatformDetails.setPlfdWeightMax20InKg(maxWeight)
                                railcarPlatformDetails.setPlfdWeightMax40InKg(maxWeight)
                            }
                        }
                    }
                }
            }
        }

        return sb.toString()
    }

}
