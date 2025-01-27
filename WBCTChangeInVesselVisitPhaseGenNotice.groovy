package WBCT

import com.navis.argo.ContextHelper
import com.navis.external.framework.util.ExtensionUtils
import com.navis.external.services.AbstractGeneralNoticeCodeExtension
import com.navis.services.business.event.GroovyEvent
import com.navis.vessel.api.VesselVisitField
import com.navis.vessel.business.schedule.VesselVisitDetails
import org.apache.log4j.Logger

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

class WBCTChangeInVesselVisitPhaseGenNotice extends AbstractGeneralNoticeCodeExtension {

    @Override
    void execute(GroovyEvent inGroovyEvent) {
        LOGGER.warn("WBCTChangeInVesselVisitPhaseGenNotice executing...")
        if (inGroovyEvent.wasFieldChanged(VesselVisitField.VVD_VISIT_PHASE.getFieldId())) {
            VesselVisitDetails vvd = (VesselVisitDetails) inGroovyEvent.getEntity()
            if (vvd != null) {
                library.sendVesselVisitMessage(vvd)
            }
        }
        //super.execute(inGroovyEvent)
    }
    def library = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "WBCTMessagingAdaptor")
    private static final Logger LOGGER = Logger.getLogger(WBCTChangeInVesselVisitPhaseGenNotice.class);

}
