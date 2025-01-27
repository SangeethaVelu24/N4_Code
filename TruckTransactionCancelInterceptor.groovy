package WBCT

import com.navis.external.road.AbstractTruckTransactionActionInterceptor
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.api.UnitManager
import com.navis.inventory.business.atoms.UnitVisitStateEnum
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.road.business.atoms.TranStatusEnum
import com.navis.road.business.atoms.TranSubTypeEnum
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckVisitDetails
import com.navis.services.business.rules.EventType
import org.apache.log4j.Level
import org.apache.log4j.Logger

/*
* @Author <a href="jprem@weservetech.com"> Dt : 05-JAN-2024
* JIRA: WBCT-79 (WBCT: Gate-08 Cancel Delivery also if Receival in Trouble.)
* Deployment Steps:
* a) Administration -> System -> Code Extensions
* b) Click on + (Add) button.
* c) Fill out all the fields as
*	. Facility
*	. TRANSACTION_ACTION_INTERCEPTOR
*	. TruckTransactionCancelInterceptor
* d) Paste the Groovy Code and click on Save.
*
*  S.No    Modified Date  Modified By               Jira      Description
*  1.      26-Sep-2024    rgopal@weservetech.com    WBCT-67   Remove genset ufv's from unitfacilityvisit which all related to cancel transaction.
*
*  S.No  Modified Date       Modified By                        Jira                         Description
*  1.    26-Sep-2024      rgopal@weservetech.com              WBCT-67    Remove genset ufv's from unitfacilityvisit which all related to cancel transaction.
*  2.    06-Nov-2024      kvijayashanthi@weservetech.com      WBCT-79    WBCT N4 - Gate-08 Cancel Delivery also if Receival in Trouble[Re-raised]
*/

class TruckTransactionCancelInterceptor extends AbstractTruckTransactionActionInterceptor {
    void preExecute(TruckTransaction truckTransaction) {
        LOGGER.setLevel(Level.DEBUG)
        LOGGER.debug("TruckTransactionCancelInterceptor preExecutions start:::::")
        if (truckTransaction != null) {
            LOGGER.warn("truck transaction " + truckTransaction)
            LOGGER.warn("truck transaction status " + truckTransaction.getTranStatus())
            if (!TranStatusEnum.CANCEL.equals(truckTransaction.getTranStatus())) {
                TruckVisitDetails truckVisitDetails = truckTransaction.getTranTruckVisit()
                if (truckVisitDetails != null) {
                    LOGGER.warn("recording custom event for THE transaction....")
                    recordCustomEvent(truckTransaction)
                    if (TranSubTypeEnum.RC.equals(truckTransaction.getTranSubType())) {
                        String accIds = truckTransaction?.getTranFlexString06()
                        if (accIds != null) {
                            String[] accessory = accIds?.split(",")
                            UnitManager unitManager = (UnitManager) Roastery.getBean(UNIT_MANAGER);
                            for (String accId : accessory) {
                                UnitFacilityVisit unitFacilityVisit = findActiveAcc(accId)
                                if (unitFacilityVisit != null) {
                                    unitManager.purgeUnit(unitFacilityVisit.getUfvUnit());
                                }
                            }
                        }
                    }

                }
            }
            HibernateApi.getInstance().save(truckTransaction)
            HibernateApi.getInstance().flush()
        }
        LOGGER.debug("TruckTransactionCancelInterceptor preExecutions ENDS::::")
    }

    void recordCustomEvent(TruckTransaction ttran) {
        if (ttran != null) {
            TruckVisitDetails tv = ttran.getTranTruckVisit()
            if (tv != null) {
                EventType eventType = EventType.findEventType(CUSTOM_TRAN_CANCEL)
                if (eventType) {
                    tv.recordTruckVisitEvent(eventType, null, String.valueOf(ttran.getTranGkey()))
                }
            }
        }
    }

    private UnitFacilityVisit findActiveAcc(String ufvId) {
        DomainQuery domainQuery = QueryUtils.createDomainQuery(UNIT_FACILITY_VISIT)
        domainQuery.addDqPredicate(PredicateFactory.eq(UnitField.UFV_UNIT_ID, ufvId));
        domainQuery.addDqPredicate(PredicateFactory.eq(UnitField.UFV_VISIT_STATE, UnitVisitStateEnum.ACTIVE));
        UnitFacilityVisit unitFacilityVisit = (UnitFacilityVisit) HibernateApi.getInstance().getUniqueEntityByDomainQuery(domainQuery)
        return unitFacilityVisit;

    }

    private static final String UNIT_FACILITY_VISIT = "UnitFacilityVisit"
    private static final String UNIT_MANAGER = "unitManager"
    private static final String CUSTOM_TRAN_CANCEL = "CUSTOM_TRAN_CANCEL"
    private static Logger LOGGER = Logger.getLogger(TruckTransactionCancelInterceptor.class)
}

