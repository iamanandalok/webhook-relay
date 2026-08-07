package dev.anandalok.webhookrelay.config;

import dev.anandalok.webhookrelay.api.HmacSignatureFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WebhookSecurityConfig {

    @Bean
    public FilterRegistrationBean<HmacSignatureFilter> hmacSignatureFilter(RelayProperties props) {
        var registration = new FilterRegistrationBean<>(new HmacSignatureFilter(props));
        registration.addUrlPatterns("/api/v1/webhooks/*");
        registration.setOrder(1);
        return registration;
    }
}
