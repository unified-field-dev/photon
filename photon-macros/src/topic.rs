//! Implementation of the [`photon::topic`] proc macro.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::Parse, parse::ParseStream, parse_macro_input, DeriveInput, Ident, LitInt, LitStr, Token,
};

/// Parses topic macro attributes: `name`, `keyed_by`, `delivery`, `shards`, `shard_by`
struct TopicAttrs {
    name: LitStr,
    keyed_by: Option<LitStr>,
    delivery: Option<LitStr>,
    shards: Option<LitInt>,
    shard_by: Option<LitStr>,
}

impl Parse for TopicAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name = None;
        let mut keyed_by = None;
        let mut delivery = None;
        let mut shards = None;
        let mut shard_by = None;

        while !input.is_empty() {
            let ident: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;

            match ident.to_string().as_str() {
                "name" => name = Some(input.parse()?),
                "keyed_by" => keyed_by = Some(input.parse()?),
                "delivery" => delivery = Some(input.parse()?),
                "shards" => shards = Some(input.parse()?),
                "shard_by" => shard_by = Some(input.parse()?),
                _ => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown attribute: {ident}"),
                    ));
                }
            }

            if !input.is_empty() {
                let _: Token![,] = input.parse()?;
            }
        }

        let name = name.ok_or_else(|| input.error("missing required attribute: name"))?;

        Ok(Self {
            name,
            keyed_by,
            delivery,
            shards,
            shard_by,
        })
    }
}

/// Implementation of the [`crate::topic`] proc-macro attribute.
pub fn topic_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = parse_macro_input!(attr as TopicAttrs);
    let input = parse_macro_input!(item as DeriveInput);

    let struct_name = &input.ident;
    let topic_name_lit = &attrs.name;
    let keyed_by_lit = attrs.keyed_by.as_ref();
    // Reserved metadata placeholder — not validated at publish time.
    let schema_json = syn::LitStr::new("{}", proc_macro2::Span::call_site());

    let delivery_mode = attrs
        .delivery
        .as_ref()
        .map_or_else(|| "broadcast".to_string(), syn::LitStr::value);

    let is_group = delivery_mode == "group" || delivery_mode == "consumer_group";

    let (keyed_by_descriptor, topic_key_extract) = keyed_by_lit.map_or_else(
        || {
            (
                quote! { None },
                quote! { let topic_key: Option<String> = None; },
            )
        },
        |kb| {
            let key_field = kb.value();
            (
                quote! { Some(#kb) },
                quote! {
                    let topic_key = match payload_json.get(#key_field) {
                        None => {
                            return Err(photon::PhotonError::Internal(format!(
                                "keyed_by field `{0}` missing from payload for topic {1}",
                                #key_field,
                                #topic_name_lit
                            )));
                        }
                        Some(v) => {
                            if let Some(s) = v.as_str() {
                                Some(s.to_string())
                            } else {
                                // Align with shard_by: stringify JSON primitives.
                                Some(v.to_string())
                            }
                        }
                    };
                },
            )
        },
    );

    let descriptor_submit = if is_group {
        let shards = attrs
            .shards
            .as_ref()
            .map_or_else(|| quote! { 32 }, |s| quote! { #s });
        let shard_by = attrs
            .shard_by
            .as_ref()
            .map_or_else(|| quote! { None }, |s| quote! { Some(#s) });
        quote! {
            photon::TopicDescriptor::group(
                #topic_name_lit,
                #shards,
                #shard_by,
                #schema_json,
            )
        }
    } else {
        quote! {
            photon::TopicDescriptor::new(
                #topic_name_lit,
                #keyed_by_descriptor,
                #schema_json,
            )
        }
    };

    let output = quote! {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        #input

        impl #struct_name {
            /// Publish this event on an explicit [`photon::Photon`] handle (preferred).
            pub async fn publish_on(self, photon: &photon::Photon) -> photon::Result<String> {
                let payload_json = serde_json::to_value(&self)?;
                let actor_json =
                    serde_json::json!({"System": {"operation": "photon_publish"}});
                #topic_key_extract
                photon.publish(
                    #topic_name_lit,
                    topic_key.as_deref(),
                    actor_json,
                    payload_json,
                ).await
            }

            /// Publish via the process-wide Photon from [`photon::configure`].
            ///
            /// Prefer [`Self::publish_on`] with an explicit handle.
            pub async fn publish(self) -> photon::Result<String> {
                let photon = photon::default().ok_or_else(|| {
                    photon::PhotonError::Internal(
                        "Photon not configured. Call photon::configure() at startup, \
                         or use publish_on(&photon)."
                            .to_string()
                    )
                })?;
                self.publish_on(&photon).await
            }

            /// Subscribe using an explicit [`photon::Photon`] handle (preferred).
            pub async fn subscribe_on(
                photon: &photon::Photon,
                opts: photon::SubscribeOpts,
            ) -> photon::Result<std::pin::Pin<std::boxed::Box<dyn futures::Stream<Item = photon::Result<photon::Envelope<#struct_name>>> + Send>>> {
                let after_seq = if opts.mode == photon::SubscriptionMode::Durable
                    && opts.subscription_name.is_some()
                {
                    let name = opts.subscription_name.as_deref().ok_or(
                        photon::PhotonError::SubscriptionNameRequired,
                    )?;
                    photon
                        .get_checkpoint_seq(
                            name,
                            #topic_name_lit,
                            opts.topic_key_filter.as_deref(),
                        )
                        .await?
                } else {
                    None
                };
                let stream = photon.subscribe(
                    #topic_name_lit,
                    opts.topic_key_filter.as_deref(),
                    after_seq,
                );
                let mapped = futures::StreamExt::map(stream, |r: photon::Result<photon::Event>| {
                    r.and_then(|ev| {
                        let payload = serde_json::from_value(ev.payload_json.clone())
                            .map_err(|e| photon::PhotonError::PayloadError(e.to_string()))?;
                        Ok(photon::Envelope { event: ev, payload })
                    })
                });
                Ok(std::boxed::Box::pin(mapped))
            }

            /// Subscribe via the process-wide Photon from [`photon::configure`].
            ///
            /// Prefer [`Self::subscribe_on`] with an explicit handle.
            pub async fn subscribe(
                opts: photon::SubscribeOpts,
            ) -> photon::Result<std::pin::Pin<std::boxed::Box<dyn futures::Stream<Item = photon::Result<photon::Envelope<#struct_name>>> + Send>>> {
                let photon = photon::default().ok_or_else(|| {
                    photon::PhotonError::Internal(
                        "Photon not configured. Call photon::configure() at startup, \
                         or use subscribe_on(&photon, opts)."
                            .to_string()
                    )
                })?;
                Self::subscribe_on(&photon, opts).await
            }
        }

        photon::inventory::submit! {
            #descriptor_submit
        }
    };

    output.into()
}
