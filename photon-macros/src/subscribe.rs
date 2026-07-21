//! Implementation of the [`photon::subscribe`] proc macro (v1 + v2 actor / injectables).

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::Parse, parse::ParseStream, parse_macro_input, FnArg, GenericArgument, Ident, ItemFn,
    LitInt, LitStr, Pat, PathArguments, Token, Type, TypePath,
};

/// Parses subscribe macro attributes: `topic`, `durable`, `group`, optional `shards`.
struct SubscribeAttrs {
    topic: LitStr,
    durable: Option<LitStr>,
    group: Option<LitStr>,
    shards: Option<LitInt>,
}

impl Parse for SubscribeAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut topic = None;
        let mut durable = None;
        let mut group = None;
        let mut shards = None;

        while !input.is_empty() {
            let ident: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;

            match ident.to_string().as_str() {
                "topic" => topic = Some(input.parse()?),
                "durable" => durable = Some(input.parse()?),
                "group" => group = Some(input.parse()?),
                "shards" => shards = Some(input.parse()?),
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

        let topic = topic.ok_or_else(|| input.error("missing required attribute: topic"))?;

        Ok(Self {
            topic,
            durable,
            group,
            shards,
        })
    }
}

/// How the actor parameter is bound after `IdentityFactory::reconstruct`.
enum ActorBinding<'a> {
    /// `Box<dyn Actor>` (path-flexible trait object).
    BoxDyn,
    /// `Arc<dyn Actor>`.
    ArcDyn,
    /// `Box<Concrete>` — downcast via `into_any`.
    BoxConcrete(&'a Type),
    /// `Arc<Concrete>` — downcast then `Arc::from`.
    ArcConcrete(&'a Type),
}

/// Optional trailing injectable after `(actor, payload)`.
enum InjectableKind {
    /// `&Event` (transport event).
    EventRef,
    /// `HandlerCtx` / `HandlerCtx<'_>` (by value).
    HandlerCtx,
    /// `&HandlerCtx` (borrow of a local ctx).
    HandlerCtxRef,
}

fn path_ends_with(path: &syn::Path, name: &str) -> bool {
    path.segments.last().is_some_and(|seg| seg.ident == name)
}

fn type_ends_with(ty: &Type, name: &str) -> bool {
    match ty {
        Type::Path(TypePath { path, .. }) => path_ends_with(path, name),
        Type::Reference(r) => type_ends_with(&r.elem, name),
        _ => false,
    }
}

fn single_angle_inner<'a>(ty: &'a Type, wrapper: &str) -> Option<&'a Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let seg = type_path.path.segments.last()?;
    if seg.ident != wrapper {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    if args.args.len() != 1 {
        return None;
    }
    match args.args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}

const fn is_dyn_trait_object(ty: &Type) -> bool {
    matches!(
        ty,
        Type::TraitObject(obj) if obj.dyn_token.is_some()
    )
}

fn classify_actor_ty(ty: &Type) -> Option<ActorBinding<'_>> {
    if let Some(inner) = single_angle_inner(ty, "Box") {
        return if is_dyn_trait_object(inner) {
            Some(ActorBinding::BoxDyn)
        } else {
            Some(ActorBinding::BoxConcrete(inner))
        };
    }
    if let Some(inner) = single_angle_inner(ty, "Arc") {
        return if is_dyn_trait_object(inner) {
            Some(ActorBinding::ArcDyn)
        } else {
            Some(ActorBinding::ArcConcrete(inner))
        };
    }
    None
}

fn classify_injectable(ty: &Type) -> Option<InjectableKind> {
    match ty {
        Type::Reference(r) if type_ends_with(&r.elem, "Event") => Some(InjectableKind::EventRef),
        Type::Path(TypePath { path, .. }) if path_ends_with(path, "HandlerCtx") => {
            Some(InjectableKind::HandlerCtx)
        }
        Type::Reference(r) if type_ends_with(&r.elem, "HandlerCtx") => {
            Some(InjectableKind::HandlerCtxRef)
        }
        _ => None,
    }
}

fn actor_binding_tokens(binding: &ActorBinding<'_>, actor_pat: &Pat) -> proc_macro2::TokenStream {
    match binding {
        ActorBinding::BoxDyn => quote! {
            let #actor_pat = identity
                .reconstruct(&actor_json)
                .map_err(|e| photon::PhotonError::Identity(e.to_string()))?;
        },
        ActorBinding::ArcDyn => quote! {
            let #actor_pat: ::std::sync::Arc<dyn photon_core::Actor> =
                ::std::sync::Arc::from(
                    identity
                        .reconstruct(&actor_json)
                        .map_err(|e| photon::PhotonError::Identity(e.to_string()))?,
                );
        },
        ActorBinding::BoxConcrete(concrete_ty) => quote! {
            let #actor_pat: ::std::boxed::Box<#concrete_ty> = identity
                .reconstruct(&actor_json)
                .map_err(|e| photon::PhotonError::Identity(e.to_string()))?
                .into_any()
                .downcast::<#concrete_ty>()
                .map_err(|_| {
                    photon::PhotonError::Identity(format!(
                        "actor JSON did not reconstruct as {}",
                        ::std::any::type_name::<#concrete_ty>()
                    ))
                })?;
        },
        ActorBinding::ArcConcrete(concrete_ty) => quote! {
            let #actor_pat: ::std::sync::Arc<#concrete_ty> = ::std::sync::Arc::from(
                identity
                    .reconstruct(&actor_json)
                    .map_err(|e| photon::PhotonError::Identity(e.to_string()))?
                    .into_any()
                    .downcast::<#concrete_ty>()
                    .map_err(|_| {
                        photon::PhotonError::Identity(format!(
                            "actor JSON did not reconstruct as {}",
                            ::std::any::type_name::<#concrete_ty>()
                        ))
                    })?,
            );
        },
    }
}

/// Implementation of the [`crate::subscribe`] proc-macro attribute.
pub fn subscribe_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = parse_macro_input!(attr as SubscribeAttrs);
    let fn_item = parse_macro_input!(item as ItemFn);

    if attrs.durable.is_some() && attrs.group.is_some() {
        return syn::Error::new_spanned(
            fn_item.sig.ident,
            "#[photon::subscribe] `durable` and `group` are mutually exclusive",
        )
        .to_compile_error()
        .into();
    }

    if attrs.durable.is_none() && attrs.group.is_none() {
        return syn::Error::new_spanned(
            fn_item.sig.ident,
            "#[photon::subscribe] requires either `durable = \"...\"` or `group = \"...\"`",
        )
        .to_compile_error()
        .into();
    }

    if fn_item.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            fn_item.sig.ident,
            "#[photon::subscribe] handlers must be async functions",
        )
        .to_compile_error()
        .into();
    }

    if fn_item.sig.inputs.len() < 2 {
        return syn::Error::new_spanned(
            fn_item.sig.ident,
            "#[photon::subscribe] handlers require (actor, event) parameters",
        )
        .to_compile_error()
        .into();
    }

    let fn_name = &fn_item.sig.ident;
    let invoke_name = Ident::new(&format!("__photon_subscribe_{fn_name}"), fn_name.span());

    // Length checked above (`inputs.len() < 2`).
    let Some(actor_arg) = fn_item.sig.inputs.first() else {
        return syn::Error::new_spanned(
            fn_item.sig.ident,
            "#[photon::subscribe] handlers require (actor, event) parameters",
        )
        .to_compile_error()
        .into();
    };
    let Some(event_arg) = fn_item.sig.inputs.iter().nth(1) else {
        return syn::Error::new_spanned(
            fn_item.sig.ident,
            "#[photon::subscribe] handlers require (actor, event) parameters",
        )
        .to_compile_error()
        .into();
    };

    let (actor_pat, actor_ty) = match actor_arg {
        FnArg::Typed(pat_type) => (&pat_type.pat, &pat_type.ty),
        FnArg::Receiver(_) => {
            return syn::Error::new_spanned(
                fn_item.sig.ident,
                "#[photon::subscribe] first parameter must be actor (not self)",
            )
            .to_compile_error()
            .into();
        }
    };

    let event_ty = match event_arg {
        FnArg::Typed(pat_type) => &pat_type.ty,
        FnArg::Receiver(_) => {
            return syn::Error::new_spanned(
                fn_item.sig.ident,
                "#[photon::subscribe] second parameter must be the event type",
            )
            .to_compile_error()
            .into();
        }
    };

    if !matches!(&**actor_pat, Pat::Ident(_)) {
        return syn::Error::new_spanned(
            actor_pat,
            "#[photon::subscribe] actor parameter must be a simple identifier",
        )
        .to_compile_error()
        .into();
    }

    let Some(actor_binding) = classify_actor_ty(actor_ty) else {
        return syn::Error::new_spanned(
            actor_ty,
            "#[photon::subscribe] actor parameter must be Box<dyn Actor>, Arc<dyn Actor>, \
             Box<Concrete>, or Arc<Concrete>",
        )
        .to_compile_error()
        .into();
    };

    let mut saw_event_ref = false;
    let mut saw_handler_ctx = false;
    let mut injectable_args: Vec<(InjectableKind, proc_macro2::TokenStream)> = Vec::new();

    for (idx, arg) in fn_item.sig.inputs.iter().enumerate().skip(2) {
        let ty = match arg {
            FnArg::Typed(pat_type) => &pat_type.ty,
            FnArg::Receiver(_) => {
                return syn::Error::new_spanned(
                    arg,
                    "#[photon::subscribe] unexpected receiver parameter",
                )
                .to_compile_error()
                .into();
            }
        };
        let Some(kind) = classify_injectable(ty) else {
            return syn::Error::new_spanned(
                ty,
                "#[photon::subscribe] unknown injectable parameter; allowed trailing \
                 injectables are `&Event` and `HandlerCtx`",
            )
            .to_compile_error()
            .into();
        };
        match kind {
            InjectableKind::EventRef => {
                if saw_event_ref {
                    return syn::Error::new_spanned(
                        ty,
                        "#[photon::subscribe] duplicate `&Event` injectable",
                    )
                    .to_compile_error()
                    .into();
                }
                saw_event_ref = true;
                let bind = format_ident!("__photon_inj_event_{idx}");
                injectable_args.push((kind, quote! { #bind }));
            }
            InjectableKind::HandlerCtx | InjectableKind::HandlerCtxRef => {
                if saw_handler_ctx {
                    return syn::Error::new_spanned(
                        ty,
                        "#[photon::subscribe] duplicate `HandlerCtx` injectable",
                    )
                    .to_compile_error()
                    .into();
                }
                saw_handler_ctx = true;
                let bind = format_ident!("__photon_inj_ctx_{idx}");
                injectable_args.push((kind, quote! { #bind }));
            }
        }
    }

    let actor_bind = actor_binding_tokens(&actor_binding, actor_pat);

    let mut injectable_binds = Vec::new();
    let mut call_extras = Vec::new();
    for (kind, bind) in &injectable_args {
        match kind {
            InjectableKind::EventRef => {
                injectable_binds.push(quote! {
                    let #bind = event;
                });
                call_extras.push(quote! { #bind });
            }
            InjectableKind::HandlerCtx => {
                injectable_binds.push(quote! {
                    let #bind = photon::HandlerCtx::from_event(event);
                });
                call_extras.push(quote! { #bind });
            }
            InjectableKind::HandlerCtxRef => {
                injectable_binds.push(quote! {
                    let #bind = photon::HandlerCtx::from_event(event);
                });
                call_extras.push(quote! { &#bind });
            }
        }
    }

    let topic_lit = &attrs.topic;
    let invoke_name_ref = &invoke_name;

    let inventory_submit = if let Some(group_lit) = &attrs.group {
        let shard_count = attrs
            .shards
            .as_ref()
            .map_or_else(|| quote! { None }, |s| quote! { Some(#s as u32) });
        let registry_key = format!("{}:group:{}", topic_lit.value(), group_lit.value());
        let registry_key_lit = LitStr::new(&registry_key, topic_lit.span());
        quote! {
            photon::HandlerDescriptor::new_group(
                #topic_lit,
                #group_lit,
                #shard_count,
                #registry_key_lit,
                #invoke_name_ref,
            )
        }
    } else {
        // `durable` required when `group` is absent (checked above).
        let Some(durable_lit) = attrs.durable.as_ref() else {
            return syn::Error::new_spanned(
                fn_item.sig.ident,
                "#[photon::subscribe] requires either `durable = \"...\"` or `group = \"...\"`",
            )
            .to_compile_error()
            .into();
        };
        let registry_key = format!("{}:{}", topic_lit.value(), durable_lit.value());
        let registry_key_lit = LitStr::new(&registry_key, topic_lit.span());
        quote! {
            photon::HandlerDescriptor::new(
                #topic_lit,
                #durable_lit,
                #registry_key_lit,
                #invoke_name_ref,
            )
        }
    };

    let output = quote! {
        #fn_item

        fn #invoke_name<'a>(
            identity: &'a dyn photon_core::IdentityFactory,
            event: &'a photon::Event,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = photon::Result<()>> + Send + 'a>> {
            Box::pin(async move {
                let actor_json = event.actor_json.to_string();
                #actor_bind
                let payload: #event_ty = serde_json::from_value(event.payload_json.clone())
                    .map_err(|e| photon::PhotonError::PayloadError(e.to_string()))?;
                #(#injectable_binds)*
                #fn_name(#actor_pat, payload, #(#call_extras),*).await
            })
        }

        photon::inventory::submit! {
            #inventory_submit
        }
    };

    output.into()
}
