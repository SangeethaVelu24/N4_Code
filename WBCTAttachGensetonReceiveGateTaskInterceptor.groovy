import com.navis.argo.ContextHelper
import com.navis.argo.business.reference.Chassis
import com.navis.argo.business.reference.Equipment
import com.navis.argo.business.reference.ScopedBizUnit
import com.navis.external.road.AbstractGateTaskInterceptor
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.units.Unit
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.workflow.TransactionAndVisitHolder
import org.apache.log4j.Logger

/**
 * Author: Monika E
 * Work Item #:WBCT-74
 * Date: 24-Apr-2024
 * Called From:
 * Description: If the previous Delivery unit has Genset, then the same Genset (previous unit Genset) should be attach to the Receival unit at the ingate.
 * PLEASE UPDATE THE BELOW HISTORY WHENEVER THE GROOVY IS MODIFIED
 * History:
 *{Date}:{Author}:{WorkItem#}:{short issue/solution description}*
 * 13-05-2024 Sangeetha Velu   WBCT-74       Refactored the code
 * 01-07-2024 Tharani G        WBCT-74       Save Previous Genset on Incoming Receive Move.
 * 26-07-2024 Lavanya V        WBCT-74       Update existing line operator on receiving or delivering the genset
 **/


class WBCTAttachGensetonReceiveGateTaskInterceptor extends AbstractGateTaskInterceptor {
    private final static Logger logger = Logger.getLogger(WBCTAttachGensetonReceiveGateTaskInterceptor.class);

    @Override
    void postProcess(TransactionAndVisitHolder inTran) {
        logger.warn("WBCTAttachGensetonReceiveGateTaskInterceptor executing...")
        if (inTran == null || inTran.getTran() == null) {
            return;
        }

        TruckTransaction truckTran = inTran.getTran();
        logger.warn("truckTran " + truckTran)
        Unit tranUnit = truckTran?.getTranUnit();
        logger.warn("tranUnit::" + tranUnit)
        Chassis tranChassis = truckTran?.getTranChassis();
        logger.warn("tranChassis::" + tranChassis)
        if (tranUnit != null && tranUnit?.getPrimaryEq() != null) {
            logger.warn("process method 1 ")
            processContainerDetails(truckTran);
        }
        if (!tranUnit && tranChassis != null) {
            logger.warn("Process method 2")
            processChassisDetails(truckTran);
        }
    }

    @Override
    void execute(TransactionAndVisitHolder inWfCtx) {
        logger.warn("execute starts::")
        super.execute(inWfCtx)
    }

    private void processChassisDetails(TruckTransaction truckTransaction) {
        logger.warn("processChassisDetails method executes::")
        Chassis tranChassis = truckTransaction.getTranChassis();
        if (tranChassis != null) {
            chassisAccessoryDetails(tranChassis?.getEqIdFull(), truckTransaction)
        }
    }

    private void chassisAccessoryDetails(String chsId, TruckTransaction truckTransaction) {
        if (chsId != null && truckTransaction != null) {
            Equipment chassisEquip = Equipment.findEquipment(chsId);
            if (chassisEquip != null) {
                Unit prevDepartedChassisUnit = findDepartedUnit(chassisEquip);
                if (prevDepartedChassisUnit != null && prevDepartedChassisUnit?.getAccessoryOnChs() != null) {
                    unitAccessoryDetails(prevDepartedChassisUnit, prevDepartedChassisUnit?.getAccessoryOnChs(), truckTransaction, truckTransaction?.getTranChsAccNbr());
                }
            }
        }
    }

    private void unitAccessoryDetails(Unit prevUnit, Unit accessory, TruckTransaction truckTran, String accessoryNbr) {
        logger.warn("prevUnit::" + prevUnit)
        logger.warn("truckTran:::" + truckTran)
        if (prevUnit != null && truckTran != null) {
            logger.warn("accessory::" + accessory)
            if (accessory != null) {
                if (accessoryNbr != null && !accessory?.getUnitId()?.equals(accessoryNbr)) {
                    logger.warn("accessoryNbr2::" + accessoryNbr)
                    logger.warn("equipment " + Equipment.findEquipment(accessoryNbr))

                    /*   DomainQuery dq = QueryUtils.createDomainQuery(ArgoRefEntity.ACCESSORY)
                               .addDqPredicate(PredicateFactory.eq(ArgoRefField.EQ_ID_FULL, accessoryNbr))
                               .addDqPredicate(PredicateFactory.eq(ArgoRefField.EQ_LIFE_CYCLE_STATE, LifeCycleStateEnum.ACTIVE))
                               .addDqPredicate(PredicateFactory.eq(ArgoRefField.EQ_CLASS, EquipClassEnum.ACCESSORY))
                       Accessory givenAccessory = (Accessory) HibernateApi.getInstance().getUniqueEntityByDomainQuery(dq)
                       logger.warn("dq " + dq)
                       //  Accessory givenAccessory = Accessory.findAccessory(accessoryNbr);
                       logger.warn("givenAccessory::" + givenAccessory)
                       if (givenAccessory == null) {
                           logger.warn("null condition::")
                           getMessageCollector().appendMessage(MessageLevel.SEVERE, PropertyKeyFactory.valueOf("INVALID_GENSET"), null, accessoryNbr);
                       } else {
                           logger.warn("Mismatch condition")
                           getMessageCollector().appendMessage(MessageLevel.SEVERE, PropertyKeyFactory.valueOf("GENSET_MISMATCH"), null, accessoryNbr, accessory?.getUnitId());
                       }*/
                    getMessageCollector().appendMessage(MessageLevel.SEVERE, PropertyKeyFactory.valueOf("GENSET_MISMATCH"), null, accessoryNbr, accessory?.getUnitId());
                } else {
                    attachAccessoryDetails(truckTran, accessory, prevUnit)
                }
            }
        }
    }

    private void attachAccessoryDetails(TruckTransaction truckTran, Unit accessory, Unit prevdepartedUnit) {
        if (truckTran != null && accessory != null) {
            if (accessory?.getPrimaryEq() == null || truckTran?.getTranUnit() == null) {
                return;
            }
            Equipment accessoryEquip = accessory?.getPrimaryEq()
            Unit gateUnit = truckTran?.getTranUnit()
            ScopedBizUnit prevDepartedAccessoryOnCtrLineOp = null
            ScopedBizUnit prevDepartedAccessoryOnChsLineOp = null

            if (prevdepartedUnit != null && accessoryEquip != null) {
                String prevDepartedAccessoryOnChs = prevdepartedUnit?.getAccessoryOnChs()?.getUnitId()
                String prevDepartedAccessoryOnCtr = prevdepartedUnit?.getAccessoryOnCtr()?.getUnitId()

                prevDepartedAccessoryOnChsLineOp = prevdepartedUnit?.getAccessoryOnChs()?.getUnitLineOperator()
                prevDepartedAccessoryOnCtrLineOp = prevdepartedUnit?.getAccessoryOnCtr()?.getUnitLineOperator()

                if (gateUnit != null) {
                    if (prevdepartedUnit?.getAccessoryOnChs() != null && !truckTran?.getTranChsAccNbr()) {
                        if (prevDepartedAccessoryOnChs != null && truckTran?.getTranChsAccNbr() == null) {
                            truckTran?.setTranChsAccNbr(prevDepartedAccessoryOnChs)
                        }
                        gateUnit?.attachAccessoryOnChassis(accessoryEquip)

                    } else if (prevdepartedUnit?.getAccessoryOnCtr() != null && !truckTran?.getTranCtrAccNbr()) {
                        if (prevDepartedAccessoryOnCtr != null && truckTran.getTranCtrAccNbr() == null) {
                            truckTran.setTranCtrAccNbr(prevDepartedAccessoryOnCtr)
                        }
                        gateUnit?.attachAccessory(accessoryEquip)
                    }
                }
            }
            HibernateApi.getInstance().save(gateUnit)

            if (gateUnit?.getAccessoryOnCtr() != null && prevDepartedAccessoryOnCtrLineOp != null) {
                gateUnit?.getAccessoryOnCtr()?.setUnitLineOperator(prevDepartedAccessoryOnCtrLineOp)
            }
            if (gateUnit?.getAccessoryOnChs() != null && prevDepartedAccessoryOnChsLineOp != null) {
                gateUnit?.getAccessoryOnChs()?.setUnitLineOperator(prevDepartedAccessoryOnChsLineOp)
            }
        }
    }

    private void processContainerDetails(TruckTransaction truckTransaction) {
        logger.warn("processContainerDetails::")
        Unit tranUnit = truckTransaction.getTranUnit();
        logger.warn("tran unit " + tranUnit)
        Chassis tranChassis = truckTransaction.getTranChassis()
        if (tranUnit != null) {
            Boolean isReeferType = tranUnit.getPrimaryEq().getEqEquipType() != null && tranUnit.getPrimaryEq().getEqEquipType().isTemperatureControlled();
            Unit prevDepartedUnit = findDepartedUnit(tranUnit.getPrimaryEq());
            logger.warn("prev departed unit " + prevDepartedUnit)
            logger.warn("prev departed ctr accessory " + prevDepartedUnit?.getAccessoryOnCtr())
            logger.warn("prev departed chs accessory " + prevDepartedUnit?.getAccessoryOnChs())
            logger.warn("truckTransaction.getTranCtrAccNbr()::" + truckTransaction?.getTranCtrAccNbr())
            logger.warn("truckTransaction.getTranChsAccNbr():::" + truckTransaction?.getTranChsAccNbr())

            if (prevDepartedUnit != null && prevDepartedUnit.getAccessoryOnCtr() != null) {
                logger.warn("truckTransaction.getTranCtrAccNbr()::" + truckTransaction.getTranCtrAccNbr())
                unitAccessoryDetails(prevDepartedUnit, prevDepartedUnit.getAccessoryOnCtr(), truckTransaction, truckTransaction.getTranCtrAccNbr());
            } else if (prevDepartedUnit != null && prevDepartedUnit.getAccessoryOnChs() != null) {
                logger.warn("truckTransaction.getTranChsAccNbr():::" + truckTransaction.getTranChsAccNbr())
                unitAccessoryDetails(prevDepartedUnit, prevDepartedUnit.getAccessoryOnChs(), truckTransaction, truckTransaction.getTranChsAccNbr());
            } else {
                logger.warn("tranChassis:::" + tranChassis)
                if (tranChassis != null) {
                    chassisAccessoryDetails(tranChassis.getEqIdFull(), truckTransaction)
                }
            }
        }
    }

    /*private void throwErrorMessage() {
        getMessageCollector().appendMessage(MessageLevel.SEVERE, PropertyKeyFactory.valueOf("GENSET_MISMATCH"), null);
    }*/

    private Unit findDepartedUnit(Equipment equip) {
        UnitFinder unitFinder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID);
        return unitFinder.findDepartedUnit(ContextHelper.getThreadComplex(), equip);
    }
}


