package com.sensorsdata.analytics.javasdk;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class InstantServlet extends TestServlet {
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        super.doPost(request, response);
        System.out.println(request.getParameter("instant_event"));
    }
}
