package WBCT

import java.text.SimpleDateFormat

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

class DateScriptRunner {

    String execute() {
        StringBuilder sb = new StringBuilder()
        String date = "2024-09-2024 18:25:30"
        sb.append("date " + date).append("\n")
        String formattedDate = null
        if (date != null) {
            try {
                Date sdfDate = sdf.parse(date)
                formattedDate = sdf.format(sdfDate)
            } catch (Exception e) {
                try {
                    Date date1 = dateFormat.parse(date)
                    formattedDate = dateFormat.format(date1) + ":00"
                } catch (Exception ex) {
                    try {
                        Date date1 = simpleDateFormat.parse(date)
                        formattedDate = simpleDateFormat.format(date1) + ":00:00"
                    } catch (Exception except) {
                        try {
                            Date date1 = format.parse(date)
                            formattedDate = format.format(date1) + " 00:00:00"
                        } catch (Exception exception) {
                            //catch
                        }
                    }
                }
            }
            sb.append("formatted date " + formattedDate)

        }
        return sb.toString()
    }

    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH");
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
}
