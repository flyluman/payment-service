package notification

import (
	"context"
	"fmt"
	"net/smtp"
)

type SMTPEmailSender struct {
	host     string
	port     int
	username string
	password string
	from     string
}

type SMTPConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
}

func NewSMTPEmailSender(cfg SMTPConfig) *SMTPEmailSender {
	return &SMTPEmailSender{
		host:     cfg.Host,
		port:     cfg.Port,
		username: cfg.Username,
		password: cfg.Password,
		from:     cfg.From,
	}
}

func (s *SMTPEmailSender) SendEmail(ctx context.Context, to, subject, bodyText, bodyHTML string) error {
	if s.host == "" {
		return fmt.Errorf("SMTP not configured")
	}

	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	auth := smtp.PlainAuth("", s.username, s.password, s.host)

	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n%s",
		s.from, to, subject, bodyText)

	if bodyHTML != "" {
		msg = fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=utf-8\r\n\r\n%s",
			s.from, to, subject, bodyHTML)
	}

	return smtp.SendMail(addr, auth, s.from, []string{to}, []byte(msg))
}
