import com.navis.argo.*
import com.navis.argo.business.api.VesselVisitFinder
import com.navis.argo.business.atoms.BizRoleEnum
import com.navis.argo.business.atoms.CarrierDirectionEnum
import com.navis.argo.business.atoms.UnitCategoryEnum
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.reference.LineOperator
import com.navis.argo.business.reference.ScopedBizUnit
import com.navis.edi.business.edimodel.EdiConsts
import com.navis.external.edi.entity.AbstractEdiPostInterceptor
import com.navis.framework.AllOtherFrameworkPropertyKeys
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.BizViolation
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.InventoryEntity
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.orders.business.eqorders.Booking
import com.navis.road.business.util.RoadBizUtil
import com.navis.vessel.business.schedule.VesselVisitDetails
import org.apache.log4j.Logger
import org.apache.xmlbeans.XmlObject

/**
 * @Copyright 2022 - Code written by WeServe LLC
 * @Author .
 *
 * Requirements :
 *
 *
 * @Inclusion Location	: Incorporated as a code extension of the type EDI_POST_INTERCEPTOR.
 * Load Code Extension to N4:
 1. Go to Administration --> System --> Code Extensions
 2. Click Add (+)
 3. Enter the values as below:
 Code Extension Name:  PAAdventReleaseValidateVvPostInterceptor
 Code Extension Type:  EDI_POST_INTERCEPTOR
 Groovy Code: Copy and paste the contents of groovy code.
 4. Click Save button

 * Attach code extension to EDI session:
 1. Go to Administration-->EDI-->EDI configuration
 2. Select the EDI session and right click on it
 3. Click on Edit
 4. Select the extension in "Post Code Extension" tab
 5. Click on save
 *
 */

class PAAdventReleaseValidateVvPostInterceptor_V1 extends AbstractEdiPostInterceptor {
    private static final Logger LOGGER = Logger.getLogger(PAAdventReleaseValidateVvPostInterceptor_V1.class)

    void beforeEdiPost(XmlObject inXmlTransactionDocument, Map inParams) {
        LOGGER.warn("PAAdventReleaseValidateVvPostInterceptor (beforeEdiPost) - Execution started.");
        if (inXmlTransactionDocument == null || !ReleaseTransactionsDocument.class.isAssignableFrom(inXmlTransactionDocument.getClass())) {
            return
        }

        ReleaseTransactionsDocument relTransDoc = (ReleaseTransactionsDocument) inXmlTransactionDocument
        ReleaseTransactionsDocument.ReleaseTransactions releaseTrans = relTransDoc.getReleaseTransactions()
        List<ReleaseTransactionDocument.ReleaseTransaction> transactionList = releaseTrans.getReleaseTransactionList()

        if (transactionList == null) {
            registerError("Release Array is NULL in before EDI post method")
            LOGGER.error("Release Array is NULL in before EDI post method")
            return
        }

        if (transactionList != null && transactionList.size() == 0) {
            registerError("Release Array is NULL in before EDI post method")
            LOGGER.error("Release Array is NULL in before EDI post method")
            return
        }

        ReleaseTransactionDocument.ReleaseTransaction releaseTransaction = transactionList.get(0)
        if (releaseTransaction != null) {
            EdiVesselVisit ediVesselVisit = releaseTransaction.getEdiVesselVisit()
            if (ediVesselVisit != null && RELEASE_TYPE_BKG.equalsIgnoreCase(releaseTransaction.getReleaseIdentifierType())) {
                CarrierVisit ediCv = null
                String bkgNbr = releaseTransaction.getEdiReleaseIdentifierList() != null ? releaseTransaction.getEdiReleaseIdentifierList().get(0).getReleaseIdentifierNbr() : null
                Booking eqo = null;
                try {
                    ShippingLine inLine = ediVesselVisit.getShippingLine()
                    ScopedBizUnit lineOp = inLine != null ? LineOperator.resolveScopedBizUnit(inLine.getShippingLineCode(), inLine.getShippingLineCodeAgency(), BizRoleEnum.LINEOP) : null
                    LineOperator lineOperator = lineOp != null ? LineOperator.resolveLineOprFromScopedBizUnit(lineOp) : null
                    if (lineOperator != null && bkgNbr != null) {
                        VesselVisitFinder vvFinder = (VesselVisitFinder) Roastery.getBean(VesselVisitFinder.BEAN_ID)
                       LOGGER.warn("ediVesselVisit.getVesselId() "+ediVesselVisit?.getVesselId())
                        ediCv =vvFinder.findVesselVisitForReleaseEdi(ContextHelper.getThreadComplex(),ediVesselVisit.getVesselId())

                      //  ediCv = vvFinder.findUniqueVesselVisitForEdi(ContextHelper.getThreadComplex(), ContextHelper.getThreadFacility(), ediVesselVisit.getVesselIdConvention(), ediVesselVisit.getVesselId(), ediVesselVisit.getOutOperatorVoyageNbr(), lineOperator, false, true)

                        //  ediCv = vvFinder.findVesselVisitForReleaseEdi(ContextHelper.getThreadComplex(),ContextHelper.getThreadFacility(),ediVesselVisit.getVesselIdConvention(),ediVesselVisit.getVesselId(),ediVesselVisit.getOutOperatorVoyageNbr(),false,lineOperator,true)
                        LOGGER.warn("PARailWayBillPostInterceptor (afterEdiPost) - cv : " + ediCv);
                        LOGGER.warn("PARailWayBillPostInterceptor (afterEdiPost) - bkgNbr : " + bkgNbr);
                        eqo = ediCv != null ? Booking.findBookingByUniquenessCriteria(bkgNbr, lineOp, ediCv) : null;
                        LOGGER.warn("PARailWayBillPostInterceptor (afterEdiPost) - eqo : " + eqo);
                        if (eqo == null) {
                            registerWarning("No matching booking found for " + bkgNbr + ". Skipped EDI processing")
                            inParams.put(EdiConsts.SKIP_POSTER, true)
                        }
                        if (ediCv != null) {
                            VesselVisitDetails vvd = VesselVisitDetails.resolveVvdFromCv(ediCv)
                            ediVesselVisit.unsetOutOperatorVoyageNbr()
                            ediVesselVisit.unsetOutVoyageNbr()
                            ediVesselVisit.setInVoyageNbr(vvd.getCarrierLineVoyNbrOrTrainId(lineOp, CarrierDirectionEnum.IB))
                            ediVesselVisit.setInOperatorVoyageNbr(vvd.getCarrierLineVoyNbrOrTrainId(lineOp, CarrierDirectionEnum.IB))
                        }
                    } else {
                        registerError("Incorrect release details received for booking: " + bkgNbr)
                    }

                } catch (BizViolation violation) {
                    LOGGER.info(violation)
                }
            }
            if (RELEASE_TYPE_UNIT.equalsIgnoreCase(releaseTransaction.getReleaseIdentifierType())) {
                String ctrNbr = releaseTransaction.getEdiReleaseIdentifierList() != null ? releaseTransaction.getEdiReleaseIdentifierList().get(0).getReleaseIdentifierNbr() : null
                if (ctrNbr != null) {
                    UnitFacilityVisit ufv = findUnitFacilityVisit(ctrNbr)
                    if (ufv == null) {
                        registerWarning("No valid unit found for " + ctrNbr + ". Skipped EDI processing")
                    }
                }
            }

        }
        LOGGER.warn("PAAdventReleaseValidateVvPostInterceptor (beforeEdiPost) - Execution completed.")
    }

    private UnitFacilityVisit findUnitFacilityVisit(String inUnitId) {
        String[] transitState = new String[4];
        transitState[0] = "S10_ADVISED";
        transitState[1] = "S20_INBOUND";
        transitState[2] = "S30_ECIN";
        transitState[3] = "S40_YARD";
        if (inUnitId != null) {
            DomainQuery dq = QueryUtils.createDomainQuery(InventoryEntity.UNIT_FACILITY_VISIT)
                    .addDqPredicate(PredicateFactory.eq(UnitField.UFV_UNIT_ID, inUnitId))
                    .addDqPredicate(PredicateFactory.eq(UnitField.UFV_CATEGORY, UnitCategoryEnum.IMPORT))
                    .addDqPredicate(PredicateFactory.in(UnitField.UFV_TRANSIT_STATE, transitState));
            List<UnitFacilityVisit> ufvList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
            if (ufvList.size() > 0) {
                return (UnitFacilityVisit) ufvList.get(0)
            }
        }
        return null;
    }

    private void registerWarning(String inWarningMessage) {
        RoadBizUtil.messageCollector.appendMessage(MessageLevel.WARNING, AllOtherFrameworkPropertyKeys.ERROR__NULL_MESSAGE, null, inWarningMessage)
    }
    private final String RELEASE_TYPE_BKG = "BKGRELEASE";
    private final String RELEASE_TYPE_UNIT = "UNITRELEASE";
}
