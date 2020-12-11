package com.thebigscale.kstreamsample.processors;

import org.apache.kafka.common.header.Headers;

public class HeaderAppender extends AbstractHeaderAppender {

    public void updateHeaders() {
        Headers headers = this.context.headers();
        System.out.println("Added header....");
        headers.add("capitalCaseKey", "capitalCaseValue".getBytes());
    }
}
