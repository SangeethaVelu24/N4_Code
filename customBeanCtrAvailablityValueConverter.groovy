/*
 * Copyright (c) 2022 WeServe LLC. All Rights Reserved.
 *
*/

import com.navis.argo.business.api.IImpediment
import com.navis.argo.business.api.ServicesManager
import com.navis.argo.business.atoms.EventEnum
import com.navis.argo.business.atoms.FlagStatusEnum
import com.navis.external.framework.beans.EBean
import com.navis.framework.business.Roastery
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.persistence.hibernate.CarinaPersistenceCallback
import com.navis.framework.persistence.hibernate.PersistenceTemplate
import com.navis.framework.presentation.FrameworkPresentationUtils
import com.navis.framework.presentation.table.DefaultValueConverter
import com.navis.framework.util.ValueHolder
import com.navis.inventory.InvField
import com.navis.inventory.business.atoms.UfvTransitStateEnum
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.services.business.api.EventManager
import com.navis.services.business.event.Event
import com.navis.services.business.rules.EventType
import com.navis.vessel.business.schedule.VesselVisitDetails
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.jetbrains.annotations.Nullable

/**
 * @Author: mailto:@weservetech.com; Date: 12-Dec-2024
 *
 *  Requirements: To update the container availability details based on the impediments fees/storage charges owing for the units.
 *
 * @Inclusion Location: Incorporated as a code extension of the type
 *
 *  Load Code Extension to N4:
 *  1. Go to Administration --> System --> Code Extensions
 *  2. Click Add (+)
 *  3. Enter the values as below:
 *     Code Extension Name: customBeanCtrAvailablityValueConverter
 *     Code Extension Type: BEAN_PROTOTYPE
 *     Groovy Code: Copy and paste the contents of groovy code.
 *  4. Click Save button
 *
 * @SetUp Value convertor configured in CUSTOM_TABLE_VIEW_AVAILABILITY
 *
 *  S.No    Modified Date   Modified By                 Jira      Description
 *
 */


class customBeanCtrAvailablityValueConverter extends DefaultValueConverter implements EBean {
    @Override
    Object convert(Object inValue, MetafieldId inColumn) {
        return super.convert(inValue, inColumn)
    }

    @Override
    Object convert(Object inValue, MetafieldId inColumn, @Nullable ValueHolder inValueHolder) {
        LOGGER.setLevel(Level.DEBUG)
        if (inColumn.getFieldId().endsWith("Synthetic")) {
            Object valueToReturn = null;
            EventManager eventManager = (EventManager) Roastery.getBean(EventManager.BEAN_ID)
            PersistenceTemplate pt = new PersistenceTemplate(FrameworkPresentationUtils.getUserContext())
            pt.invoke(new CarinaPersistenceCallback() {
                @Override
                protected void doInTransaction() {
                    Long ufvGkey = inValueHolder.getFieldValue(InvField.UFV_GKEY) as Long
                    if (ufvGkey != null) {
                        UnitFacilityVisit ufv = UnitFacilityVisit.hydrate(ufvGkey)
                        if (ufv != null) {
                            Unit unit = ufv?.getUfvUnit()
                            VesselVisitDetails vesselVisitDetails = ufv.getUfvActualIbCv() != null ? VesselVisitDetails.resolveVvdFromCv(ufv.getUfvActualIbCv()) : null

                            switch (inColumn.getFieldId()) {
                                case "ufvVesselNameSynthetic":
                                    valueToReturn = vesselVisitDetails?.getCarrierVehicleName()
                                    break;
                                case "ufvIBVoySynthetic":
                                    valueToReturn = vesselVisitDetails?.getCarrierIbVoyNbrOrTrainId();
                                    break
                                case "ufvDischDateSynthetic":
                                    if (ufv.isTransitStateBeyond(UfvTransitStateEnum.S30_ECIN)) {

                                        EventType unitDischEvnt = EventType.findEventType(EventEnum.UNIT_DISCH.getKey());
                                        if (unitDischEvnt != null) {
                                            Event event = eventManager.getMostRecentEventByType(unitDischEvnt, ufv.getUfvUnit());
                                            if (event != null) {
                                                valueToReturn = event?.getEvntAppliedDate()
                                            }
                                        }
                                    }
                                    break;
                                case "ufvCustomsHoldSynthetic":
                                    String customStatus = getImpedimentForUnit(unit, "CUSTOMS")
                                    if (customStatus != null) {
                                        valueToReturn = NG + NOT_RELEASED
                                    } else {
                                        valueToReturn = OK
                                    }
                                    break;
                                case "ufvFreightHoldSynthetic":
                                    String blfreightStatus = getImpedimentForUnit(unit, "FREIGHT_HOLD")
                                    if (blfreightStatus != null) {
                                        valueToReturn = NG + NOT_RELEASED
                                    } else {
                                        valueToReturn = OK
                                    }
                                    break;
                                case "ufvHoldStatusSynthetic":
                                    String holdStatus = unit.getUnitActiveImpediments()
                                    // getImpedimentForUnit(unit, null)
                                    LOGGER.debug("hold status " + holdStatus)
                                    if (holdStatus != null) {
                                        valueToReturn = "HOLD"
                                    } else {
                                        valueToReturn = "RELEASED"
                                    }
                                    break;

                                case "ufvContainerAvailabilitySynthetic":
                                    String isHoldExisit = unit.getUnitActiveImpediments()
                                    if (isHoldExisit != null) {
                                        valueToReturn = NO
                                        break;
                                    }
                                    valueToReturn = YES
                                    break;
                            }
                        }
                    }
                }
            })
            return valueToReturn
        }

        return super.convert(inValue, inColumn, inValueHolder)
    }

    @Override
    String getDetailedDiagnostics() {
        return "customBeanCtrAvailablityValueConverter"
    }

    private String getImpedimentForUnit(Unit unit, String flagView) {
        ServicesManager servicesManager = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID)
        Collection<IImpediment> impedimentsCollection = (Collection<IImpediment>) servicesManager.getImpedimentsForEntity(unit)
        String flagType = null
        String flagActive = null
        String[] flags = [flagView]
        for (IImpediment impediment : impedimentsCollection) {
            if (impediment != null && FlagStatusEnum.ACTIVE.equals(impediment.getStatus()) || FlagStatusEnum.REQUIRED.equals(impediment.getStatus())) {
                flagType = impediment.getFlagType()?.getId()
                if (flagType != null && flags.contains(flagType)) {
                    flagActive = impediment.getFlagType()?.getId()
                }
            }
        }
        return flagActive
    }

    private static final String NG = "NG "
    private static final String OK = "OK "
    private static final String YES = "YES "
    private static final String NO = "NO "
    private static final String NOT_RELEASED = "NOT RELEASED"
    private static final Logger LOGGER = Logger.getLogger(customBeanCtrAvailablityValueConverter.class)
}
