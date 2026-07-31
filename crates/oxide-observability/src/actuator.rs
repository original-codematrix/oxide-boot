inventory::submit! {
    oxide_web::ManagementRoute {
        path: "/actuator/prometheus",
        register: |_ctx, router| {
            Ok(router.route(
                "/actuator/prometheus",
                oxide_web::__private::axum::routing::get(|| async {
                    (
                        [(oxide_web::__private::axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                        crate::metrics::prometheus_handle().render(),
                    )
                }),
            ))
        },
    }
}
