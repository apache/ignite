package org.apache.ignite.internal.ducktest.tests.pds_compatibility_test;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

public class Account implements Serializable {
    @QuerySqlField(index = true, inlineSize = 48)
    private String firstName;
    @QuerySqlField(index = true, inlineSize = 48)
    private String lastName;
    @QuerySqlField(index = true, inlineSize = 48)
    private String catName;
    @QuerySqlField(index = true, inlineSize = 48)
    private String dogName;
    @QuerySqlField(index = true, inlineSize = 48)
    private String city;
    @QuerySqlField(index = true, inlineSize = 48)
    private String country;
    @QuerySqlField(index = true, inlineSize = 48)
    private String eMail;
    @QuerySqlField(index = true, inlineSize = 48)
    private String phoneNumber;
    @QuerySqlField(index = true, inlineSize = 48)
    private String socialNumber;
    @QuerySqlField(index = true, inlineSize = 48)
    private String postIndex;
    public long balance;

    public Account(String firstName, String lastName, String catName, String dogName, String city, String country, String eMail, String phoneNumber, String socialNumber, String postIndex, long balance) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.catName = catName;
        this.dogName = dogName;
        this.city = city;
        this.country = country;
        this.eMail = eMail;
        this.phoneNumber = phoneNumber;
        this.socialNumber = socialNumber;
        this.postIndex = postIndex;
        this.balance = balance;
    }

    @Override
    public String toString() {
        return "Account{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", catName='" + catName + '\'' +
                ", dogName='" + dogName + '\'' +
                ", city='" + city + '\'' +
                ", country='" + country + '\'' +
                ", eMail='" + eMail + '\'' +
                ", phoneNumber='" + phoneNumber + '\'' +
                ", socialNumber='" + socialNumber + '\'' +
                ", postIndex='" + postIndex + '\'' +
                ", balance=" + balance +
                '}';
    }
}
