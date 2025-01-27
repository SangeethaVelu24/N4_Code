

import com.navis.external.road.AbstractGateTaskInterceptor
import com.navis.framework.util.internationalization.PropertyKey
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageLevel
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.util.RoadBizUtil
import com.navis.road.business.workflow.TransactionAndVisitHolder
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger

import java.text.SimpleDateFormat
import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * @Author: uaarthi@weservetech.com; Date: 9/26/2024
 *
 *  Requirements: Add Early Appointment error at pregate.
 *
 * @Inclusion Location: Incorporated as a code extension of the type
 *
 *  Load Code Extension to N4:
 *  1. Go to Administration --> System --> Code Extensions
 *  2. Click Add (+)
 *  3. Enter the values as below:
 *     Code Extension Name:PARejectInvalidAppointmentGateTask
 *     Code Extension Type:  GATE_TASK_INTERCEPTOR
 *     Groovy Code: Copy and paste the contents of groovy code.
 *  4. Click Save button
 *
 *
 *
 */
class PARejectInvalidAppointmentGateTask extends  AbstractGateTaskInterceptor{

    private static Logger LOGGER = Logger.getLogger(PARejectInvalidAppointmentGateTask.class);

    @Override
    void execute(TransactionAndVisitHolder inWfCtx) {
        LOGGER.setLevel(Level.DEBUG)
        TruckTransaction transaction = inWfCtx.getTran();
        if (transaction == null) {
            return;
        }
        // Throw an error
        //APPT_EARLY||2014-10-14T04:000700_0800
        String apptError = transaction.getTranFlexString08()
        String slot = ""
        String delay = "0mins"

        LOGGER.debug("PARejectInvalidAppointmentGateTask apptError ::" + apptError)

        if (apptError != null && StringUtils.isNotEmpty(apptError.trim())) {
            if (apptError.contains('|')) {
                String[] apptSlotError = apptError.split('\\|')
                apptError = apptSlotError[0]
                LOGGER.debug("PARejectInvalidAppointmentGateTask apptError ::" + apptError)
                slot = apptSlotError[1] != null ? apptSlotError[1].replace('_', "-") : ""
                if (apptError.contains("LATE") && apptSlotError.length > 2) {
                    String apptDate = apptSlotError[2]
                    delay = calculateDelay(apptDate,true)
                } else  if (apptError.contains("EARLY") && apptSlotError.length > 2) {
                    String apptDate = apptSlotError[2]
                    delay = calculateDelay(apptDate,false)
                }

            }
            PropertyKey apptErrorKey = apptError != null ? PropertyKeyFactory.valueOf(apptError) : null;
            if (apptErrorKey != null) {
                RoadBizUtil.getMessageCollector().appendMessage(MessageLevel.SEVERE, apptErrorKey, null, slot, delay);
            }

        }
    }

    String calculateDelay(String apptDate, boolean isLate ) {
        String calculatedDelay = "0mins"
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
            LocalDateTime parsedDateTime = LocalDateTime.parse(apptDate, formatter);
            LocalDateTime currentDateTime = LocalDateTime.now().withSecond(0).withNano(0);
            LOGGER.debug("PARejectInvalidAppointmentGateTask parsedDateTime ::" + parsedDateTime)
            LOGGER.debug("PARejectInvalidAppointmentGateTask currentDateTime ::" + currentDateTime)
            if (isLate && parsedDateTime.isBefore(currentDateTime)) {
                LOGGER.debug("parsedDateTime is before...")
                Duration duration = Duration.between(parsedDateTime, currentDateTime);
                LOGGER.debug("duration current , parsed "+Duration.between(parsedDateTime, currentDateTime))
                LOGGER.debug("duration "+duration)
                long totalMinutes = duration.toMinutes();
                long hours = totalMinutes / 60;
                long minutes = totalMinutes % 60;
                if (hours > 0) {
                    calculatedDelay = String.format("%dhr %dmins", hours, minutes);
                } else {
                    calculatedDelay = String.format("%dmins", minutes);
                }
                LOGGER.debug("calculatedDelay "+calculatedDelay)
            } else  if (!isLate && parsedDateTime.isAfter(currentDateTime)) {
                LOGGER.debug("parsedDateTime is after...")
                Duration duration = Duration.between( currentDateTime,parsedDateTime);
                LOGGER.debug("duration "+duration)

                long totalMinutes = duration.toMinutes();
                long hours = totalMinutes / 60;
                long minutes = totalMinutes % 60;
                if (hours > 0) {
                    calculatedDelay = String.format("%dhr %dmins", hours, minutes);
                } else {
                    calculatedDelay = String.format("%dmins", minutes);
                }
                LOGGER.debug("calculatedDelay "+calculatedDelay)
            }
        } catch (Exception inEx) {
            //Do nothing
            LOGGER.debug("Error while parsing date " + inEx.toString());
        }
        return calculatedDelay
    }


}
