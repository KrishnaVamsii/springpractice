package com.practice.spring.basic.springpractice.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
import javax.persistence.*;
import java.util.Date;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor

public class UserRoles {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    private Integer id;
    private String userId;
    private String roleName;
    private String roleDescription;
    private String ica;
    private String status;

    @Basic(optional = false)
    @Column(insertable = false, updatable = false)
    @CreationTimestamp
    private Date createdTimestamp;
    private String lastUpdatedTimestamp;

}