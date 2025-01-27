import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.CarrierVisitPhaseEnum
import com.navis.argo.business.reference.Equipment
import com.navis.extension.portal.ExtensionBeanUtils
import com.navis.extension.portal.IExtensionTransactionHandler
import com.navis.external.argo.AbstractGroovyJobCodeExtension
import com.navis.framework.business.Roastery
import com.navis.framework.extension.FrameworkExtensionTypes
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.UserContext
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.presentation.context.PresentationContextUtils
import com.navis.framework.presentation.context.RequestContext
import com.navis.framework.util.BizViolation
import com.navis.framework.util.message.MessageCollector
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.atoms.UfvTransitStateEnum
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.road.RoadField
import com.navis.road.business.api.RoadManager
import com.navis.road.business.atoms.TranStatusEnum
import com.navis.road.business.atoms.TruckVisitStatusEnum
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckVisitDetails
import com.navis.road.business.reference.CancelReason
import org.apache.log4j.Level
import org.apache.log4j.Logger

/*
 *
 * @Author <ahref="mailto:vsangeetha@weservetech.com"  >Sangeetha V</a>,
 * Date : 06/05/2024
 * @Inclusion Location : Incorporated as a code extension of the type GROOVY_JOB_CODE_EXTENSION. Copy -->Paste this code(PATruckVisitAutoCloseGroovyJob.groovy)
 * Requirements : This groovy is used to update TruckVisit and associated Transaction status to CLOSED if it is created before one month.
 * @Set up Groovy Job AUTO_CLOSE_TRUCK_VISIT_OLD_RECORDS
 *
 *S.No      Modified Date      Modified By          Jira                      Description
 * 1        01/July/2024      Vijaya Shanthi K     WBCT - 82       WBCT N4 - Gate-11 Automatically cancel open transactions on Shift End [Failed Cases]
 * 2        01/Aug/2024       GopalaKrishnan R     WBCT - 82       Removed (Commented) validation to update the Vesselphase to "Complete"  when truckvisit state is not complete.
 * 3        16/Dec/2024       Sangeetha Velu       WBCT - 81       When the transaction gets cancelled, sends the notification to GOS
 */

class WBCTTruckVisitAutoCloseGroovyJob extends AbstractGroovyJobCodeExtension {
    @Override
    void execute(Map<String, Object> inParams) {
        LOGGER.setLevel(Level.DEBUG)
        LOGGER.debug("WBCTTruckVisitAutoCloseGroovyJob executes...")
        List<TruckVisitDetails> tvList = getTruckVisit()
        Map parms = new HashMap();
        Map results = new HashMap();
        final IExtensionTransactionHandler handler = ExtensionBeanUtils.getExtensionTransactionHandler();
        RequestContext requestContext = PresentationContextUtils.getRequestContext();
        UserContext context = requestContext?.getUserContext();
        if (tvList != null && !tvList.isEmpty()) {
            for (TruckVisitDetails truckVisitDetails : tvList) {
                int cancelCount = 0;
                int completeCount = 0;

                if (truckVisitDetails != null) {
                    Set<TruckTransaction> truckTranSet = truckVisitDetails.getTvdtlsTruckTrans()
                    if (truckTranSet != null && !truckTranSet.isEmpty()) {
                        for (TruckTransaction transaction : truckTranSet) {
                            boolean cancelTransaction = false;
                            boolean closeTransaction = false;
                            boolean closeTruckVisit = false;
                            boolean isReceival = transaction.isReceival();
                            boolean isDelivery = transaction.isDelivery();

                            UnitFacilityVisit ufv = null;
                            if (transaction.getTranUfv() != null) {
                                ufv = transaction.getTranUfv();
                            } else {
                                String ctrNbr = transaction.getTranCtrNbr() != null ? transaction.getTranCtrNbr() : transaction.getTranCtrNbrAssigned()
                                if (ctrNbr != null) {
                                    Equipment equipment = Equipment.findEquipment(ctrNbr)
                                    if (equipment != null) {
                                        Unit unit = unitFinder.findActiveUnit(ContextHelper.getThreadComplex(), equipment, transaction.getTranUnitCategory())
                                        if (unit != null) {
                                            ufv = unit.getUnitActiveUfvNowActive()
                                        } else {
                                            Unit departedUnit = unitFinder.findDepartedUnit(ContextHelper.getThreadComplex(), equipment)
                                            if (departedUnit != null) {
                                                ufv = departedUnit.getUfvForFacilityCompletedOnly(ContextHelper.getThreadFacility());
                                            }
                                        }
                                    }
                                } else if (transaction.getTranStatus().equals(TranStatusEnum.TROUBLE)) {
                                    closeTransaction = true;
                                }
                            }
                            if (transaction.getTranStatus().equals(TranStatusEnum.TROUBLE) && ufv == null) {
                                closeTransaction = true;
                            }

                            if (ufv != null) {
                                try {
                                    switch (transaction.getTranStatus()) {

                                        case TranStatusEnum.TROUBLE:
                                            if (isReceival) {
                                                if (UfvTransitStateEnum.S30_ECIN.equals(ufv.getUfvTransitState()) || UfvTransitStateEnum.S40_YARD.equals(ufv.getUfvTransitState())) {
                                                    closeTransaction = true;
                                                }
                                            }
                                            if (isDelivery) {
                                                if (ufv.isTransitStateBeyond(UfvTransitStateEnum.S20_INBOUND)) {
                                                    closeTransaction = true;
                                                }
                                            }
                                            if (UfvTransitStateEnum.S70_DEPARTED.equals(ufv.getUfvTransitState()) && (TruckVisitStatusEnum.OK.equals(truckVisitDetails.getTvdtlsStatus()) || TruckVisitStatusEnum.TROUBLE.equals(truckVisitDetails.getTvdtlsStatus()))) {
                                                closeTransaction = true;
                                                closeTruckVisit = true
                                            } else {
                                                closeTransaction = true;
                                            }
                                            break;

                                        case TranStatusEnum.OK:
                                            if (isReceival) {
                                                if ((UfvTransitStateEnum.S30_ECIN.equals(ufv.getUfvTransitState())
                                                        || UfvTransitStateEnum.S40_YARD.equals(ufv.getUfvTransitState()))) {
                                                    if (UfvTransitStateEnum.S30_ECIN.equals(ufv.getUfvTransitState())) {
                                                        transaction.setTranStatus(TranStatusEnum.CLOSED)
                                                    } else {
                                                        closeTransaction = true;
                                                    }
                                                }
                                            }
                                            if (isDelivery) {
                                                if (UfvTransitStateEnum.S40_YARD.equals(ufv.getUfvTransitState())
                                                        || UfvTransitStateEnum.S50_ECOUT.equals(ufv.getUfvTransitState())
                                                        || UfvTransitStateEnum.S60_LOADED.equals(ufv.getUfvTransitState())) {
                                                    closeTransaction = true;
                                                } else if (UfvTransitStateEnum.S70_DEPARTED.equals(ufv.getUfvTransitState())) {
                                                    transaction.setTranStatus(TranStatusEnum.COMPLETE)
                                                    completeCount = completeCount + 1;
                                                }
                                            }
                                            break;

                                        case TranStatusEnum.CANCEL:
                                            cancelTransaction = true
                                            cancelCount = cancelCount + 1;
                                            break;

                                        case TranStatusEnum.COMPLETE:
                                            if (isReceival) {
                                                if (UfvTransitStateEnum.S70_DEPARTED.equals(ufv.getUfvTransitState()) && (TruckVisitStatusEnum.OK.equals(truckVisitDetails.getTvdtlsStatus()) || TruckVisitStatusEnum.TROUBLE.equals(truckVisitDetails.getTvdtlsStatus()))) {
                                                    closeTruckVisit = true
                                                }
                                            }
                                            completeCount = completeCount + 1
                                            break;

                                        default:
                                            if (TranStatusEnum.CLOSED || TranStatusEnum.RETURNING || TranStatusEnum.INCOMPLETE) {
                                                closeTransaction = true;
                                            }
                                            break;
                                    }
                                } catch (BizViolation bv) {
                                    LOGGER.error("Violation occured for Unit.. " + bv)
                                }
                            }

                            if (cancelTransaction) {
                                transaction.setTranStatus(TranStatusEnum.CANCEL)
                                parms.put(MAP_KEY, transaction?.getTranGkey());
                                MessageCollector mc = handler.executeInTransaction(ContextHelper?.getThreadUserContext(),
                                        FrameworkExtensionTypes.TRANSACTED_BUSINESS_FUNCTION, TRANSACTION_BUSINESS, parms, results);
                            }
                            if (closeTransaction) {
                                transaction.setTranStatus(TranStatusEnum.CLOSED)
                            }
                            HibernateApi.getInstance().save(transaction)
                            if (closeTruckVisit) {
                                truckVisitDetails.setTvdtlsStatus(TruckVisitStatusEnum.CLOSED)
                            }
                        }
                        if (!truckVisitDetails.hasPendingTransaction()) {
                            if (truckVisitDetails.getTransactionCount() == completeCount || (truckVisitDetails.getTransactionCount() == cancelCount && TruckVisitStatusEnum.COMPLETE.equals(truckVisitDetails.getTvdtlsStatus()))) {

                                if (truckVisitDetails.getTransactionCount() == completeCount && TruckVisitStatusEnum.OK.equals(truckVisitDetails.getTvdtlsStatus())) {
                                    truckVisitDetails.setTvdtlsStatus(TruckVisitStatusEnum.CLOSED)
                                }
                                if (!TruckVisitStatusEnum.COMPLETE.equals(truckVisitDetails.getTvdtlsStatus())) {
                                    //truckVisitDetails.getCvdCv().setCvVisitPhase(CarrierVisitPhaseEnum.COMPLETE);
                                    //truckVisitDetails.getCvdCv().departFromFacility(null);
                                    truckVisitDetails.setTvdtlsChanged(new Date(ArgoUtils.timeNow().getTime()));
                                    truckVisitDetails.setTvdtlsStatus(TruckVisitStatusEnum.CLOSED)
                                    truckVisitDetails.setTvdtlsExitedYard(new Date(ArgoUtils.timeNow().getTime()));
                                }
                            } else if (truckVisitDetails.getTransactionCount() == cancelCount) {
                                LOGGER.warn("Inside no pending txn and not complete tv status")
                                if (!TruckVisitStatusEnum.CANCEL.equals(truckVisitDetails.getTvdtlsStatus())) {
                                    truckVisitDetails.setTvdtlsStatus(TruckVisitStatusEnum.CANCEL)
                                }
                            } else if (!TruckVisitStatusEnum.COMPLETE.equals(truckVisitDetails.getTvdtlsStatus())) {
                                LOGGER.warn("multiple txn in different gate txn status and tv as OK")
                                if (!TruckVisitStatusEnum.CLOSED.equals(truckVisitDetails.getTvdtlsStatus())) {
                                    truckVisitDetails.setTvdtlsStatus(TruckVisitStatusEnum.CLOSED)
                                }
                            }
                            HibernateApi.getInstance().save(truckVisitDetails)
                        }
                    } else {
                        if (TruckVisitStatusEnum.OK.equals(truckVisitDetails.getTvdtlsStatus()) || TruckVisitStatusEnum.TROUBLE.equals(truckVisitDetails.getTvdtlsStatus())) {
                            truckVisitDetails.cancelTruckVisit()
                        }
                    }


                }
                HibernateApi.getInstance().flush();
            }


        }
    }

    private List getTruckVisit() {
        DomainQuery domainQuery = QueryUtils.createDomainQuery("TruckVisitDetails")
                .addDqPredicate(PredicateFactory.in(RoadField.TVDTLS_STATUS, [TruckVisitStatusEnum.OK, TruckVisitStatusEnum.TROUBLE, TruckVisitStatusEnum.COMPLETE]))
                .addDqPredicate(PredicateFactory.between(RoadField.TVDTLS_CREATED, getLastMonth(), getStartDate()))
                .addDqOrdering(Ordering.desc(RoadField.TVDTLS_CREATED))
        return HibernateApi.getInstance().findEntitiesByDomainQuery(domainQuery)
    }

    private Date getLastMonth() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR, -240);
        return cal.getTime();
    }

    private Date getStartDate() {
        Calendar cal = Calendar.getInstance();
        return cal.getTime();
    }

    private static RoadManager getRoadManager() {
        return (RoadManager) Roastery.getBean(RoadManager.BEAN_ID)
    }

    private static final String TRANSACTION_BUSINESS = "PAGateTranCancelFormTransactionBusiness";
    final CancelReason reason = CancelReason.findOrCreate("OG_CANCEL", "Transaction cancelled by job")
    private static final String MAP_KEY = "gkeys";
    private UnitFinder unitFinder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID);
    private static final Logger LOGGER = Logger.getLogger(WBCTTruckVisitAutoCloseGroovyJob.class)
}