package WBCT

import com.navis.argo.business.api.ArgoUtils
import com.navis.external.argo.AbstractGroovyJobCodeExtension
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.inventory.InvEntity
import com.navis.inventory.InvField
import com.navis.inventory.business.atoms.UfvTransitStateEnum
import com.navis.inventory.business.atoms.UnitVisitStateEnum
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.services.business.rules.EventType

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 14-Nov-2024
*
*  Requirements: Record the custom event for yard unit
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: WBCTUpdateWheeledOrDeckedPositionGroovyJob
*     Code Extension Type: GROOVY_JOB_CODE_EXTENSION
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTUpdateWheeledOrDeckedPositionGroovyJob extends AbstractGroovyJobCodeExtension {

    @Override
    void execute(Map<String, Object> inParams) {
        List<UnitFacilityVisit> ufvList = retrieveYardUnits()

        if (ufvList != null && ufvList.size() > 0) {
            for (UnitFacilityVisit ufv : ufvList) {
                Unit unit = ufv.getUfvUnit()
                EventType eventType = EventType.findEventType(YARD_SIDE_MANUAL_UPDATE)
                if (unit != null && eventType != null) {
                    unit.recordEvent(eventType, null, "Updated through groovy", ArgoUtils.timeNow())
                }
            }
        }
    }

    List<UnitFacilityVisit> retrieveYardUnits() {
        DomainQuery dq = QueryUtils.createDomainQuery(InvEntity.UNIT_FACILITY_VISIT)
                .addDqPredicate(PredicateFactory.eq(InvField.UFV_VISIT_STATE, UnitVisitStateEnum.ACTIVE))
                .addDqPredicate(PredicateFactory.eq(InvField.UFV_TRANSIT_STATE, UfvTransitStateEnum.S40_YARD))

        return HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
    }

    private static final String YARD_SIDE_MANUAL_UPDATE = "YARD_SIDE_MANUAL_UPDATE"
}
