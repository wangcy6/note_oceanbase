#ifndef USSL_HOOK_LOOP_USSL_LOOP_
#define USSL_HOOK_LOOP_USSL_LOOP_

// the client communicates with the background thread through this pipe
int client_comm_pipe[2];

extern int ussl_loop_add_listen_once(int listen_fd, int backlog);
extern int ussl_loop_add_clientfd(int client_fd, uint64_t gid, int ctx_id, int send_negotiation, int auth_methods, int epfd,
                                  struct epoll_event *event);
int  __attribute__((weak)) dispatch_accept_fd_to_certain_group(int fd, uint64_t gid);
extern void add_to_timeout_list(ussl_dlink_t *l);
extern void remove_from_timeout_list(ussl_dlink_t *l);
extern void check_and_handle_timeout_event();
extern int init_bg_thread();
#endif // USSL_HOOK_LOOP_USSL_LOOP_
