/*
 * Copyright (c) 2018 Navis LLC. All Rights Reserved.
 */


import com.navis.argo.*
import com.navis.argo.business.api.ArgoEdiFacade
import com.navis.argo.business.atoms.BizRoleEnum
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.model.Complex
import com.navis.argo.business.reference.*
import com.navis.cargo.business.model.BillOfLading
import com.navis.external.edi.entity.AbstractEdiPostInterceptor
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernatingEntity
import com.navis.framework.util.BizViolation
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.atoms.UnitVisitStateEnum
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFinderPea
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.xmlbeans.XmlObject
import org.jetbrains.annotations.Nullable

/*
 * Version #: 8.1.9.0
 * Author: Copied from NOLA PROD
 * Work Item #: Backlog Item 216269:TAMPA N4 - Manifest Post Interceptor
 * Description: See below
 *
 * PLEASE UPDATE THE BELOW HISTORY WHENEVER THE GROOVY IS MODIFIED
 * History:
 * {Date}:{Author}:{WorkItem#}:{short issue/solution description}
 */

/*
 *******************************
 * Original comments from NOLA *
 *******************************
 * If the container's iso group is a tank set the container's owner and operator to the line.
 *
 * @author <a href="weservetech">weserve</a>
 *
 * Date: 10/17/2024
 * JIRA: WBCT-233
 *
 * Invoked from : Manfiest EDI session
 *
 * ---------------------------------------------------------------------------------------------------------------------------------------------------
 * Revision History
 * ---------------------------------------------------------------------------------------------------------------------------------------------------
 * 10/17/2024 SangeethaVelu - Update the unit visit state as Advised, if the message function is delete
 */

class PAManifestPostInterceptor extends AbstractEdiPostInterceptor {
    @Override
    public void beforeEdiPost(XmlObject inXmlTransactionDocument, Map inParams) {
        log("Calling beforeEdiPost method.");
        logger.setLevel(Level.DEBUG)
        if (BlTransactionsDocument.class.isAssignableFrom(inXmlTransactionDocument.getClass())) {
            BlTransactionsDocument billOfLadingDoc = (BlTransactionsDocument) inXmlTransactionDocument;
            BlTransactionsDocument.BlTransactions billOfLadingTrans = billOfLadingDoc.getBlTransactions();
            BlTransactionDocument.BlTransaction billOfLadingTran = billOfLadingTrans.getBlTransactionArray(0);
            EdiVesselVisit vesselVisit = billOfLadingTran.getEdiVesselVisit();
            if (vesselVisit != null) {
                ShippingLine shippingLine = vesselVisit.getShippingLine();
                if (shippingLine != null) {
                    String vesselOperatorId = shippingLine.getShippingLineCode();
                    String vesselOperatorIdAgency = shippingLine.getShippingLineCodeAgency();
                    ScopedBizUnit line = ScopedBizUnit.resolveScopedBizUnit(vesselOperatorId, vesselOperatorIdAgency, BizRoleEnum.LINEOP);
                    if (line != null) {
                        BlTransactionDocument.BlTransaction.EdiBlItemHolder[] blItemHolders = billOfLadingTran.getEdiBlItemHolderArray();
                        if (blItemHolders != null) {
                            for (BlTransactionDocument.BlTransaction.EdiBlItemHolder blItemHolder : blItemHolders) {
                                EdiBlEquipment[] ediBlEquipments = blItemHolder.getEdiBlEquipmentArray();
                                if (ediBlEquipments != null) {
                                    for (EdiBlEquipment ediBlEquipment : ediBlEquipments) {
                                        EdiContainer ediContainer = ediBlEquipment.getEdiContainer();

                                        //SpecialStow Validation
                                        if (!billOfLadingTran.getMsgFunction().equals("DELETE")) {
                                            List<SpecialStowInstruction> instructionList = ediContainer?.getSpecialStowInstructionsList()
                                            if (instructionList != null && instructionList.size() > 0) {
                                                for (SpecialStowInstruction specialStowInstruction : instructionList) {
                                                    String specialStow = SpecialStow.findSpecialStow(specialStowInstruction?.getId())
                                                    inParams.put("SPL_STOW", specialStow)
                                                    if (specialStow.equals(null)) {
                                                        getMessageCollector().appendMessage(MessageLevel.SEVERE, PropertyKeyFactory.valueOf(SPECIALSTOW_NOT_IN_N4), null)
                                                    }
                                                }
                                            }
                                        }


                                        if (ediContainer != null) {
                                            String containerNbr = ediContainer.getContainerNbr();
                                            Container container = Container.findContainer(containerNbr);
                                            if (container != null) {
                                                Object obj = getLibrary("PAUpdateEquipOwnerAndOperator");
                                                obj.execute(container, line);
                                            }
                                            // Set the tare weight as the gross, if gross is not sent
                                            if (zeroWeight.equals(ediContainer.getContainerGrossWt())) {
                                                ediContainer.setContainerGrossWt(ediContainer.getContainerTareWt());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        logger.warn("Completed Calling beforeEdiPost method.");
    }

    @Override
    void afterEdiPost(XmlObject inXmlTransactionDocument, HibernatingEntity inHibernatingEntity, Map inParams) {
        logger.setLevel(Level.DEBUG)
        if (BlTransactionsDocument.class.isAssignableFrom(inXmlTransactionDocument.getClass())) {
            BlTransactionsDocument billOfLadingDoc = (BlTransactionsDocument) inXmlTransactionDocument;
            BlTransactionsDocument.BlTransactions billOfLadingTrans = billOfLadingDoc.getBlTransactions();
            BlTransactionDocument.BlTransaction billOfLadingTran = billOfLadingTrans.getBlTransactionArray(0);
            EdiVesselVisit vesselVisit = billOfLadingTran.getEdiVesselVisit();
            if (vesselVisit != null) {
                CarrierVisit ediCv = vesselVisit != null ? this.resolveCarrierVisit(vesselVisit) : null
                if (ediCv != null) {
                    BillOfLading billOfLading = BillOfLading.findBillOfLading(billOfLadingTran.getEdiBillOfLading()?.getBlNbr(), ediCv)
                    if (billOfLading != null) {
                        String splStow = (String) inParams.get("SPL_STOW")
                        if (splStow != null) {
                            billOfLading.setBlNotes(splStow.replace("SpecialStow Id:", ""))
                        }

                        if (DELETE.equals(billOfLadingTran.getMsgFunction())) {
                            List<BlTransactionDocument.BlTransaction.EdiBlItemHolder> ediBlItemHolderList = billOfLadingTran.getEdiBlItemHolderList();
                            BlTransactionDocument.BlTransaction.EdiBlItemHolder ediBlItemHolder = ediBlItemHolderList.size() > 0 ? ediBlItemHolderList.get(0) : null;
                            if (ediBlItemHolder != null) {
                                List<EdiBlEquipment> ediBlEquipmentList = ediBlItemHolder.getEdiBlEquipmentList();
                                EdiBlEquipment ediBlEquipment = ediBlEquipmentList.size() > 0 ? ediBlEquipmentList.get(0) : null;
                                if (ediBlEquipment != null) {
                                    EdiContainer ediContainer = ediBlEquipment.getEdiContainer();
                                    Equipment equipment = Equipment.findEquipment(ediContainer?.getContainerNbr());
                                    if (equipment != null) {
                                        UnitFinder unitFinder = (UnitFinder) Roastery.getBean(UnitFinderPea.BEAN_ID);
                                        Unit unit = unitFinder.findActiveUnit(ContextHelper.getThreadComplex(), equipment);
                                        if (unit != null) {
                                            unit.setUnitVisitState(UnitVisitStateEnum.ADVISED)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private CarrierVisit resolveCarrierVisit(@Nullable EdiVesselVisit inEdiVv) throws BizViolation {
        Complex inComplex = ContextHelper.getThreadComplex()
        ShippingLine inLine = inEdiVv?.getShippingLine()
        ScopedBizUnit lineOp = inLine != null ? LineOperator.resolveScopedBizUnit(inLine.getShippingLineCode(), inLine.getShippingLineCodeAgency(), BizRoleEnum.LINEOP) : null
        if (inComplex == null) {
            logger.warn(" Thread Complex is Null")
        }
        if (lineOp == null) {
            return null
        }

        LineOperator lineOperator = LineOperator.resolveLineOprFromScopedBizUnit(lineOp)
        CarrierVisit carrierVisit = getArgoEdiFacade().findVesselVisit(ContextHelper.getThreadEdiPostingContext(),
                inComplex, ContextHelper.getThreadFacility(), inEdiVv, lineOperator, true);

        return carrierVisit
    }

    protected static ArgoEdiFacade getArgoEdiFacade() {
        return (ArgoEdiFacade) Roastery.getBean(ArgoEdiFacade.BEAN_ID)

    }

    private static String DELETE = "DELETE"
    private String zeroWeight = "0.0";
    private String SPECIALSTOW_NOT_IN_N4 = "Couldn't find the Given Special StowId."
    private static final Logger logger = Logger.getLogger(PAManifestPostInterceptor.class)
}
