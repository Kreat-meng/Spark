package com.atguigu.shizhan;

import lombok.Data;

import java.io.Serializable;
@Data
public class CategoryCountInfo implements Serializable, Comparable<CategoryCountInfo> {

    private String categoryId;
    private Long clickCount;
    private Long orderCount;
    private Long payCount;

    public CategoryCountInfo() {
    }

    public CategoryCountInfo(String categoryId, Long clickCount, Long orderCount, Long payCount) {
        this.categoryId = categoryId;
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public int compareTo(CategoryCountInfo o) {
        if (this.getClickCount().equals(o.getClickCount())){
            if (this.getOrderCount().equals(o.getOrderCount())){
                if (this.getPayCount().equals(o.getPayCount())){
                    return 0;
                }else return this.getPayCount() > o.getPayCount() ? 1 : -1;
            }else return this.getOrderCount() > o.getOrderCount() ? 1 : -1;

        }else return this.getClickCount() > o.getClickCount() ? 1 : -1;
    }
}
