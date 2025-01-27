import com.navis.argo.*
import com.navis.argo.business.api.ArgoEdiFacade
import com.navis.argo.business.api.ArgoEdiUtils
import com.navis.argo.business.atoms.DataSourceEnum
import com.navis.argo.business.atoms.FreightKindEnum
import com.navis.argo.business.atoms.LocTypeEnum
import com.navis.argo.business.atoms.UnitCategoryEnum
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.model.Facility
import com.navis.argo.business.model.GeneralReference
import com.navis.argo.business.reference.Container
import com.navis.argo.business.reference.Equipment
import com.navis.argo.business.reference.LineOperator
import com.navis.argo.business.reference.RoutingPoint
import com.navis.argo.business.reference.ScopedBizUnit
import com.navis.external.edi.entity.AbstractEdiPostInterceptor
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.persistence.HibernatingEntity
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.api.UnitManager
import com.navis.inventory.business.atoms.UfvTransitStateEnum
import com.navis.inventory.business.atoms.UnitVisitStateEnum
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit

/*
 * Copyright (c) 2024 WeServe LLC. All Rights Reserved.
 *
 */

import com.navis.inventory.business.units.UnitFinderPea
import com.navis.inventory.business.units.UnitManagerPea
import com.navis.orders.business.eqorders.Booking
import com.navis.orders.business.eqorders.EquipmentOrderItem
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.xmlbeans.XmlObject

import java.text.SimpleDateFormat

/*
 *
 * @Author <a href="mailto:kgopinath@weservetech.com">Gopinath K</a>, 18/Oct/2024
 *
 * Requirements :
 *
 * Rail Hub Update: Enhancement to search the matching routing point id from general reference and update it against Origin and Destination.
 *                  In addition to that, original Rail Hub (Destination) to be updated in unit flex field
 *
 * @Inclusion Location	: Incorporated as a code extension of the type EDI_POST_INTERCEPTOR.
 *
 *  Load Code Extension to N4:
        1. Go to Administration --> System --> Code Extensions
        2. Click Add (+)
        3. Enter the values as below:
            Code Extension Name:  PARailWayBillPostInterceptor
            Code Extension Type:  EDI_POST_INTERCEPTOR
            Groovy Code: Copy and paste the contents of groovy code.
        4. Click Save button

 Attach code extension to EDI session:
        1. Go to Administration-->EDI-->EDI configuration
        2. Select the EDI session and right click on it
        3. Click on Edit
        4. Select the extension in "Post Code Extension" tab
        5. Click on save
 *
 *Modified @Author <ahref="mailto:vsangeetha@weservetech.com">Sangeetha Velu </a>
 * Date: 18/Oct/2024
 * Requirements:-  finds the vessel visit without line operator
 */

class PARailWayBillPostInterceptor extends AbstractEdiPostInterceptor {
    private static Logger LOGGER = Logger.getLogger(PARailWayBillPostInterceptor.class);

    @Override
    public void beforeEdiPost(XmlObject inXmlTransactionDocument, Map inParams) {
        LOGGER.setLevel(Level.DEBUG);
        LOGGER.debug("PARailWayBillPostInterceptor (beforeEdiPost) - Execution started.");
        if (RailWayBillTransactionsDocument.class.isAssignableFrom(inXmlTransactionDocument.getClass())) {
            RailWayBillTransactionsDocument railWayBillDocument = (RailWayBillTransactionsDocument) inXmlTransactionDocument;
            RailWayBillTransactionsDocument.RailWayBillTransactions railWayBillTrans = railWayBillDocument != null ? railWayBillDocument.getRailWayBillTransactions() : null;
            List<RailWayBillTransactionDocument.RailWayBillTransaction> railWayBillTransList = railWayBillTrans != null ? railWayBillTrans.getRailWayBillTransactionList() : null;

            RailWayBillTransactionDocument.RailWayBillTransaction railWayBillTransaction = railWayBillTransList != null && railWayBillTransList.size() > 0 ? railWayBillTransList.get(0) : null
            if (railWayBillTransaction == null) {
                registerError("No transaction data available, cannot process EDI.")
                return
            }

            //Code added for testing - End
            String category = null;
            EdiCommodity ediCommodity = null

            if (railWayBillTransaction.getEdiRailWayBillContainer() != null) {
                ediCommodity = railWayBillTransaction.getEdiRailWayBillContainer().getEdiCommodity()
                if (railWayBillTransaction.getEdiRailWayBillContainer().getEdiContainer() != null) {
                    railWayBillTransaction.getEdiRailWayBillContainer().getEdiContainer().setContainerNbr(Container.padCheckDigit(
                            railWayBillTransaction.getEdiRailWayBillContainer().getEdiContainer().getContainerNbr()))
                    category = railWayBillTransaction.getEdiRailWayBillContainer().getEdiContainer().getContainerCategory()
                }
            }
            //  EdiCommodity ediCommodity = railWayBillTransaction.getEdiRailWayBillContainer() != null ? railWayBillTransaction.getEdiRailWayBillContainer().getEdiCommodity() : null
            Port originPort = ediCommodity != null ? ediCommodity.getOriginPort() : null
            Port destinationPort = ediCommodity != null ? ediCommodity.getDestinationPort() : null
            EdiFlexFields ediFlexFields = railWayBillTransaction.getEdiFlexFields()
            if (ediFlexFields == null) {
                ediFlexFields = railWayBillTransaction.addNewEdiFlexFields()

            }
            boolean isOffDock = false
            String originRailHub = null;
            String destinationRailHub = null;

            if (originPort != null && !originPort.getPortId().isEmpty()) {
                originRailHub = originPort.getPortId()
                LOGGER.debug("PARailWayBillPostInterceptor (beforeEdiPost) - Origin Rail Hub: " + originPort.getPortId());

                GeneralReference originGenRef = GeneralReference.findUniqueEntryById("WBCT", "RAIL_EDI", "RAIL_HUB", originPort.getPortId());
                LOGGER.debug("originGenRef::" + originGenRef)


                if (originGenRef != null && originGenRef.getRefValue1() != null) {
                    LOGGER.debug("PARailWayBillPostInterceptor (beforeEdiPost) - Origin Port Id: " + originGenRef.getRefValue1());
                    originPort.setPortId(originGenRef.getRefValue1())
                }
                if (originRailHub != null && (UnitCategoryEnum.IMPORT.getKey().equalsIgnoreCase(category) || UnitCategoryEnum.STORAGE.getKey().equalsIgnoreCase(category))) {
                    GeneralReference onOffDockGenRef = GeneralReference.findUniqueEntryById("WBCT", "RAILWAYBILL", "ON_OFF_DOCK", null);
                    LOGGER.debug("onOffDockGenRef::" + onOffDockGenRef)

                    if (onOffDockGenRef != null) {
                        LOGGER.debug("Ref value 1 ::" + onOffDockGenRef.getRefValue1())
                        LOGGER.debug("Ref value 2 ::" + onOffDockGenRef.getRefValue2())
                        LOGGER.debug("Port id ::" + originPort.getPortId())
                        LOGGER.debug("contains off dog id  ::" + (onOffDockGenRef.getRefValue2().contains(originPort.getPortId())))
                        if (onOffDockGenRef.getRefValue1() != null && (!onOffDockGenRef.getRefValue1().isEmpty()) && onOffDockGenRef.getRefValue1().contains(originRailHub)) {
                            //On dock container received
                            ediFlexFields.setUnitFlexString02("ON")
                        } else if (onOffDockGenRef.getRefValue2() != null && (!onOffDockGenRef.getRefValue2().isEmpty()) && onOffDockGenRef.getRefValue2().contains(originRailHub)) {
                            //Off dock container received
                            //  ediFlexFields.setUnitFlexString02("OFF")
                            isOffDock = true
                            inParams.put("OFF_DOCK", isOffDock)
                            LOGGER.debug("EdiWay bill ::" + railWayBillTransaction.getEdiWayBill())
                            if (railWayBillTransaction.getEdiWayBill() != null) {
                                LOGGER.debug("WayBillNbr::" + railWayBillTransaction.getEdiWayBill().getWayBillNbr())
                                if (railWayBillTransaction.getEdiWayBill().getWayBillNbr() != null && (!railWayBillTransaction.getEdiWayBill().getWayBillNbr().isEmpty())) {
                                    inParams.put("WAY_BILL_NBR", railWayBillTransaction.getEdiWayBill().getWayBillNbr())
                                }
                                LOGGER.debug("WayBillDate::" + railWayBillTransaction.getEdiWayBill().getWayBillDate())
                                if (railWayBillTransaction.getEdiWayBill().getWayBillDate() != null) {
                                    inParams.put("WAY_BILL_DATE", railWayBillTransaction.getEdiWayBill().getWayBillDate())
                                }
                            }
                        }
                    }
                }
            }
            if (destinationPort != null && !destinationPort.getPortId().isEmpty()) {
                destinationRailHub = destinationPort.getPortId()
                LOGGER.debug("PARailWayBillPostInterceptor (beforeEdiPost) - Destination Rail Hub: " + destinationPort.getPortId());
                ediFlexFields.setUnitFlexString01(destinationPort.getPortId())
                GeneralReference destGenRef = GeneralReference.findUniqueEntryById("WBCT", "RAIL_EDI", "RAIL_HUB", destinationPort.getPortId());
                if (destGenRef != null && destGenRef.getRefValue1() != null) {
                    LOGGER.debug("PARailWayBillPostInterceptor (beforeEdiPost) - Destination Port Id: " + destGenRef.getRefValue1());
                    destinationPort.setPortId(destGenRef.getRefValue1())

                }
            }
            //Gopal - Do not create Advised import unit by Waybill
            Facility facility = ArgoEdiUtils.findFacility(ContextHelper.getThreadEdiPostingContext(), railWayBillTransaction.getVesselCallFacility());
            if (facility == null) {
                facility = Facility.findFacility("WBCT");
            }
            String containerNbr = railWayBillTransaction.getEdiRailWayBillContainer().getEdiContainer().getContainerNbr()
            Unit unit = null;
            if (containerNbr != null) {
                Equipment equipment = Equipment.findEquipment(containerNbr);
                if (equipment != null) {
                    UnitFinder unitFinder = Roastery.getBean(UnitFinder.BEAN_ID);
                    unit = unitFinder.findActiveUnit(ContextHelper.getThreadComplex(), equipment);
                    if (unit == null) {
                        unit = unitFinder.findAttachedUnit(ContextHelper.getThreadComplex(), equipment);
                        if (unit != null && (UnitVisitStateEnum.DEPARTED.equals(unit.getUnitVisitState())
                                || UnitVisitStateEnum.RETIRED.equals(unit.getUnitVisitState()))) {
                            unit = null;
                        }
                    } /*else if (unit != null && unit.getUnitActiveUfvNowActive() != null && isOffDock) {
                        inParams.put("UFV_GKEY", unit.getUnitActiveUfvNowActive().getUfvGkey())
                    }*/
                }

                //Check if it is for import containers, container should be already created and
                // transit state should not be beyond yard
                if (facility != null && facility.getFcyRoutingPoint() != null && originPort != null) {
                    RoutingPoint originRtg = ArgoEdiUtils.extractRoutingPoint(ContextHelper.getThreadEdiPostingContext(),
                            originPort, UnitField.UNIT_ORIGIN);

                    if (facility != null && facility.getFcyRoutingPoint() != null &&
                            facility.getFcyRoutingPoint().equals(originRtg)) {

                        //only if advised or active import unit exists, post the Waybill, if not throw error
                        //is this check redundant?
                        if (unit == null || (unit != null && (!UnitCategoryEnum.IMPORT.equals(unit.getUnitCategory())
                                || !(unit.isActive() || UnitVisitStateEnum.ADVISED.equals(unit.getUnitVisitState()))))) {
                            registerError("Import unit does not exists for " + containerNbr)
                        }
                        if (unit != null && unit.getUnitActiveUfvNowActive() != null) {
                            UnitFacilityVisit unitFacilityVisit = unit.getUnitActiveUfvNowActive();
                            if (unitFacilityVisit != null && unitFacilityVisit.isTransitStateBeyond(UfvTransitStateEnum.S40_YARD)) {
                                registerError("Import unit is beyond yard stage " + containerNbr);
                            }
                        }
                    }
                }
            }
            LOGGER.debug("isOffDock::" + isOffDock)
            LOGGER.debug("is origin port same ::" + (!CURRENT_PORT_ID.equalsIgnoreCase(originPort.getPortId())))
            LOGGER.debug("is destination  port same ::" + (!CURRENT_PORT_ID.equalsIgnoreCase(destinationPort.getPortId())))
            LOGGER.debug("originRailHub::" + originRailHub)
            LOGGER.debug("destinationRailHub::" + destinationRailHub)

            if (isOffDock && originRailHub != null && destinationRailHub != null && (!CURRENT_PORT_ID.equalsIgnoreCase(originRailHub))
                    && (!CURRENT_PORT_ID.equalsIgnoreCase(destinationRailHub))) {
                LOGGER.debug("EDI poster is skipped::")
                inParams.put("ORIGIN", originRailHub)
                inParams.put("DESTINATION", destinationRailHub)
                inParams.put("SKIP_POSTER", true)
            }

        }
        LOGGER.debug("PARailWayBillPostInterceptor (beforeEdiPost) SKIP_POST:: " + inParams.get("SKIP_POSTER"))
        LOGGER.debug("PARailWayBillPostInterceptor (beforeEdiPost) - Execution completed.");
    }


    void afterEdiPost(XmlObject inXmlTransactionDocument, HibernatingEntity inHibernatingEntity, Map inParams) {
        LOGGER.setLevel(Level.DEBUG);
        LOGGER.debug("PARailWayBillPostInterceptor (afterEdiPost) - Execution started.");
        if (RailWayBillTransactionsDocument.class.isAssignableFrom(inXmlTransactionDocument.getClass())) {
            RailWayBillTransactionsDocument railWayBillDocument = (RailWayBillTransactionsDocument) inXmlTransactionDocument;
            RailWayBillTransactionsDocument.RailWayBillTransactions railWayBillTrans = railWayBillDocument != null ? railWayBillDocument.getRailWayBillTransactions() : null;
            List<RailWayBillTransactionDocument.RailWayBillTransaction> railWayBillTransList = railWayBillTrans != null ? railWayBillTrans.getRailWayBillTransactionList() : null;
            RailWayBillTransactionDocument.RailWayBillTransaction railWayBillTransaction = railWayBillTransList != null && railWayBillTransList.size() > 0 ? railWayBillTransList.get(0) : null
            if (railWayBillTransaction != null) {
                EdiVesselVisit ediVesselVisit = railWayBillTransaction.getEdiVesselVisit()
                CarrierVisit ediCv = null;
                String bkgNbr = railWayBillTransaction.getBookingNbr();
                Booking eqo = null;
                try {
                    if (ediVesselVisit != null) {
                        ArgoEdiFacade argoEdiFacade = Roastery.getBean(ArgoEdiFacade.BEAN_ID)
                        ediCv = argoEdiFacade.findVesselVisit(ContextHelper.getThreadEdiPostingContext(), ContextHelper.getThreadComplex(), ContextHelper.getThreadFacility(), ediVesselVisit, null, false)
                        LOGGER.warn("edi cv " + ediCv)
                        eqo = ediCv != null ? Booking.findBookingWithoutLine(bkgNbr, ediCv) : null;
                        LOGGER.warn("booking with out line " + Booking.findBookingWithoutLine(bkgNbr, ediCv))
                        LOGGER.debug("PARailWayBillPostInterceptor (afterEdiPost) - eqo : " + eqo);
                    }
                    EdiContainer ediContainer = railWayBillTransaction.getEdiRailWayBillContainer() != null? railWayBillTransaction.getEdiRailWayBillContainer().getEdiContainer() : null
                    //String ctrNbr = railWayBillTransaction.getEdiRailWayBillContainer() != null && railWayBillTransaction.getEdiRailWayBillContainer().getEdiContainer() != null ? railWayBillTransaction.getEdiRailWayBillContainer().getEdiContainer().getContainerNbr() : null
                    String ctrNbr = ediContainer != null? ediContainer.getContainerNbr() : null
                    EdiOperator ediOperator = ediContainer != null? ediContainer.getContainerOperator() : null
                    ScopedBizUnit lineOp
                    if (ediContainer != null)
                        lineOp = findLineOperatorById(ediOperator.getOperator(), ediOperator.getOperatorCodeAgency())

                    LOGGER.debug("PARailWayBillPostInterceptor (afterEdiPost) - ctrNbr : " + ctrNbr + ", lineOp: "+lineOp);
                    if (ctrNbr != null && !ctrNbr.isEmpty()) {
                        Equipment equipment = Equipment.findEquipment(ctrNbr)
                        UnitFinder unitFinder = Roastery.getBean(UnitFinderPea.BEAN_ID)
                        Unit unit = equipment != null ? unitFinder.findActiveUnit(ContextHelper.getThreadComplex(), equipment, UnitCategoryEnum.EXPORT) : null
                        if (unit == null) {
                            LOGGER.debug("threadFcy: "+ContextHelper.getThreadFacility());
                            UnitFacilityVisit ufv = unitFinder.findPreadvisedUnit(ContextHelper.getThreadFacility(), equipment, UnitCategoryEnum.EXPORT)
                            unit = ufv != null? ufv.getUfvUnit() : null
                            if (unit == null) {
                                UnitManager unitManager = Roastery.getBean(UnitManagerPea.BEAN_ID)
                                ufv = unitManager.findOrCreatePreadvisedUnit(ContextHelper.getThreadFacility(),
                                        equipment.getEqIdFull(),
                                        equipment.getEqEquipType(),
                                        UnitCategoryEnum.EXPORT,
                                        FreightKindEnum.FCL,
                                        lineOp,
                                        null,
                                        ediCv,
                                        DataSourceEnum.EDI_RLWAYBILL,
                                        null)
                                unit = ufv != null? ufv.getUfvUnit() : null
                            }
                        }
                        LOGGER.debug("unit::" + unit)
                        if (unit != null) {
                            LOGGER.debug(" container OFF_DOCK key ::" + inParams.containsKey("OFF_DOCK"))
                            if (inParams.containsKey("OFF_DOCK")) {
                                if (unit.getUnitActiveUfvNowActive() != null) {
                                    unit.getUnitActiveUfvNowActive().updateObCv(CarrierVisit.findOrCreateGenericCv(ContextHelper.getThreadComplex(), LocTypeEnum.TRUCK))
                                }
                                unit.setUnitFlexString02("OFF")
                                LOGGER.debug("Destination::" + inParams.get("DESTINATION"))
                                if (unit.getUnitGoods() != null && inParams.containsKey("DESTINATION")) {
                                    unit.getUnitGoods().setGdsDestination((String) inParams.get("DESTINATION"))
                                }
                                LOGGER.debug("Origin :: " + inParams.get("ORIGIN"))
                                if (inParams.containsKey("ORIGIN")) {
                                    unit.setUnitFlexString01((String) inParams.get("ORIGIN"))
                                }
                                LOGGER.debug("Way Bill Nbr  :: " + inParams.containsKey("WAY_BILL_NBR"))
                                if (inParams.containsKey("WAY_BILL_NBR")) {
                                    unit.setUnitWayBillNbr((String) inParams.get("WAY_BILL_NBR"))
                                }
                                LOGGER.debug("WAY_BILL_DATE :: " + inParams.containsKey("WAY_BILL_DATE"))

                                if (inParams.containsKey("WAY_BILL_DATE")) {
                                    unit.setUnitWayBillDate(DATE_FORMAT.parse((String) inParams.get("WAY_BILL_DATE")))
                                }
                            }
                            if (eqo != null && unit.getUnitCategory().equals(eqo.getEqoCategory())) {
                                LOGGER.debug("PARailWayBillPostInterceptor (afterEdiPost) - unit : " + unit);
                                EquipmentOrderItem orderItem = eqo.findMatchingItemReceive(unit, false, false, false)
                                LOGGER.debug("PARailWayBillPostInterceptor (afterEdiPost) - orderItem : " + orderItem);
                                if (orderItem != null && unit.getUnitEquipment() != null) {
                                    unit.assignToOrder(orderItem, unit.getUnitEquipment())
                                }
                                LOGGER.debug("PARailWayBillPostInterceptor (afterEdiPost) - unit.getArrivalOrder() : " + unit.getArrivalOrder());
                            }
                        }
                    }
                } catch (Exception e) {
                    registerError("PARailWayBillPostInterceptor - Exception: " + e);
                }
            }

        }
    }

    public static LineOperator findLineOperatorById(String inLineId, String code) {
        DomainQuery domainQuery = QueryUtils.createDomainQuery("LineOperator")
        if (SCAC.equals(code))
            domainQuery.addDqPredicate(PredicateFactory.eq(ArgoRefField.BZU_SCAC, inLineId))
        else if (BIC.equals(code))
            domainQuery.addDqPredicate(PredicateFactory.eq(ArgoRefField.BZU_BIC, inLineId))

        return (LineOperator) HibernateApi.getInstance().getUniqueEntityByDomainQuery(domainQuery)
    }

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyy-MM-dd")
    private static final String CURRENT_PORT_ID = "LAX";
    private static final String SCAC = "SCAC";
    private static final String BIC = "BIC";
}
