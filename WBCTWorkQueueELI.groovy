import com.navis.argo.ContextHelper
import com.navis.argo.business.atoms.LocTypeEnum
import com.navis.external.framework.entity.AbstractEntityLifecycleInterceptor
import com.navis.external.framework.entity.EEntityView
import com.navis.external.framework.util.EFieldChanges
import com.navis.external.framework.util.EFieldChangesView
import com.navis.external.framework.util.ExtensionUtils
import com.navis.inventory.MovesField
import com.navis.inventory.business.moves.WorkQueue
import org.apache.log4j.Logger

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date:  25-Oct-2024
*  Requirements: WBCT-227 - push Vessel visit and crane data from N4 to API endpoints
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: WBCTCraneUpdateELI
*     Code Extension Type: ENTITY_LIFECYCLE_INTERCEPTION
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTWorkQueueELI extends AbstractEntityLifecycleInterceptor {
    @Override
    void onUpdate(EEntityView inEntity, EFieldChangesView inOriginalFieldChanges, EFieldChanges inMoreFieldChanges) {
        WorkQueue workQueue = (WorkQueue) inEntity._entity
        LOGGER.warn("WQ_FIRST_RELATED_SHIFT_PKEY " + (inOriginalFieldChanges.hasFieldChange(MovesField.WQ_FIRST_RELATED_SHIFT_PKEY)))
        if (workQueue != null && LocTypeEnum.VESSEL.equals(workQueue.getWqPosLocType())) {
            if (inOriginalFieldChanges.hasFieldChange(MovesField.WQ_IS_BLUE) && (Boolean.TRUE.equals(inOriginalFieldChanges.findFieldChange(MovesField.WQ_IS_BLUE).getNewValue()))
                    || (inOriginalFieldChanges.hasFieldChange(MovesField.WQ_FIRST_RELATED_SHIFT_PKEY) && workQueue.getWqIsBlue()) ) {
                try {
                    library.processCraneDetails(workQueue)
                } catch (Exception e) {
                    LOGGER.warn("sendCraneUpdateMessage catch executing.." + e.getMessage())
                }
            }
        }
    }
    def library = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "WBCTMessagingAdaptor")
    private static final Logger LOGGER = Logger.getLogger(WBCTWorkQueueELI.class);
}
