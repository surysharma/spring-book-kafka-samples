package com.thebigscale.kstreamsample.promotion.dto;

import lombok.Data;

@Data
public class PromotionMessage {
    private String clientId;
    private String promotionName;
    private String promotionValue;

}
