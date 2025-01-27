package WBCT

import com.navis.external.argo.AbstractGroovyJobCodeExtension
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.rail.RailEntity
import com.navis.rail.RailField
import com.navis.rail.business.entity.Railcar
import com.navis.rail.business.entity.RailcarDetails
import com.navis.rail.business.entity.RailcarPlatform
import com.navis.rail.business.entity.RailcarPlatformDetails

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

class WBCTUpdateRailCarsMaxWeightGroovyJob extends AbstractGroovyJobCodeExtension {

    @Override
    void execute(Map<String, Object> inParams) {
        Double safeWeight = 0.0

        Double maxWeight = 0.0
        DomainQuery dq = QueryUtils.createDomainQuery(RailEntity.RAILCAR)
                .addDqPredicate(PredicateFactory.isNotNull(RailField.RCAR_ID))
               // .addDqPredicate(PredicateFactory.isNotNull(RailCompoundField.RCAR_PLF_WEIGHT_MAX20))
        .addDqPredicate(PredicateFactory.between(RailField.RCAR_GKEY,6204471,185521296))

                .setDqMaxResults(2500)
        List<Railcar> railcarList = (List<Railcar>) HibernateApi.getInstance().findEntitiesByDomainQuery(dq)

        if (railcarList != null && railcarList.size() > 0) {
            for (Railcar railcar : railcarList) {
                //  Railcar railcar = railcarList.get(0)

                RailcarDetails railcarDetails = railcar.getRcarRailcarDetails()
                if (railcarDetails != null && railcarDetails.getRcdSafeWeightKg() != null) {
                    safeWeight = railcarDetails.getRcdSafeWeightKg()
                    Set<RailcarPlatform> railcarTypePlatformSet = (Set<RailcarPlatform>) railcar.getRcarPlatforms()
                    if (railcarTypePlatformSet != null && railcarTypePlatformSet.size() > 0) {
                        int railCarPlatFormSize = railcarTypePlatformSet.size()
                        maxWeight = safeWeight / railCarPlatFormSize
                        for (RailcarPlatform railcarTypePlatform : railcarTypePlatformSet) {
                            RailcarPlatformDetails railcarPlatformDetails = railcarTypePlatform.getRcarplfRailcarPlatformDetails()
                            if (railcarPlatformDetails != null) {
                                if (railcarPlatformDetails.getPlfdWeightMax20InKg() == null && railcarPlatformDetails.getPlfdWeightMax40InKg() == null) {
                                    railcarPlatformDetails.setPlfdWeightMax20InKg(maxWeight)
                                    railcarPlatformDetails.setPlfdWeightMax40InKg(maxWeight)
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
