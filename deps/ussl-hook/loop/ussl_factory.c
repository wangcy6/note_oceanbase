static clientfd_sk_t *clientfd_sk_new(ussl_sf_t *sf)
{
  clientfd_sk_t *s = NULL;
  if (NULL == (s = (typeof(s))malloc(sizeof(clientfd_sk_t)))) {
    ussl_log_error("malloc for clientfd_sk_t failed, errno:%d", errno);
  } else {
    s->fty = sf;
    s->handle_event = (ussl_handle_event_t)clientfd_sk_handle_event;
    s->type = CLIENT_SOCK;
    ussl_dlink_init(&s->timeout_link);
  }
  return s;
}

static void clientfd_sk_delete(ussl_sf_t *sf, clientfd_sk_t *s)
{
  if (NULL != s) {
    free(s);
  }
}

int clientfd_sf_init(ussl_sf_t *sf)
{
  ussl_sf_init(sf, (void *)clientfd_sk_new, (void *)clientfd_sk_delete);
  return 0;
}

static acceptfd_sk_t *acceptfd_sk_new(ussl_sf_t *sf)
{
  acceptfd_sk_t *s = NULL;
  if (NULL == (s = (typeof(s))malloc(sizeof(acceptfd_sk_t)))) {
    ussl_log_error("malloc for acceptfd_sk_t failed, errno:%d", errno);
  } else {
    s->fty = sf;
    s->handle_event = (ussl_handle_event_t)acceptfd_sk_handle_event;
    s->type = SERVER_SOCK;
    s->start_time = time(NULL);
    ussl_dlink_init(&s->timeout_link);
  }
  return s;
}

static void acceptfd_sk_delete(ussl_sf_t *sf, acceptfd_sk_t *s)
{
  if (NULL != s) {
    free(s);
  }
}

int acceptfd_sf_init(ussl_sf_t *sf)
{
  ussl_sf_init(sf, (void *)acceptfd_sk_new, (void *)acceptfd_sk_delete);
  return 0;
}