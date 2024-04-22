package worker

import (
	"context"

	"github.com/carv-protocol/verifier/internal/conf"
	"github.com/carv-protocol/verifier/internal/data"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

var ProviderSet = wire.NewSet(NewWorkerServer)

type Server struct {
	cf                       *conf.Bootstrap
	data                     *data.Data
	logger                   *log.Helper
	verifierRepo             *data.VerifierRepo
	transactionRepo          *data.TransactionRepo
	reportTeeAttestationRepo *data.ReportTeeAttestationEventRepo
	chains                   *Chains
}

func NewWorkerServer(bootstrap *conf.Bootstrap, data *data.Data, logger *log.Helper, verifierRepo *data.VerifierRepo, transactionRepo *data.TransactionRepo, reportTeeAttestationRepo *data.ReportTeeAttestationEventRepo) *Server {
	w := &Server{
		cf:                       bootstrap,
		data:                     data,
		logger:                   logger,
		verifierRepo:             verifierRepo,
		transactionRepo:          transactionRepo,
		reportTeeAttestationRepo: reportTeeAttestationRepo,
	}

	return w
}

func (s *Server) Start(ctx context.Context) error {
	s.logger.WithContext(ctx).Info("worker server starting")

	chain, err := NewChain(ctx, s.cf, s.data, s.logger, s.verifierRepo, s.transactionRepo, s.reportTeeAttestationRepo)
	if err != nil {
		return err
	}
	for i := 0; i < len(s.cf.Chains); i++ {

		if err = chain.Start(ctx, i); err != nil {
			return err
		}
	}

	s.chains = chain

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.logger.WithContext(ctx).Info("worker server stopping")

	if err := s.chains.Stop(ctx, 0); err != nil {
		s.logger.WithContext(ctx).Error(err)
	}

	s.logger.WithContext(ctx).Info("worker server stopped")
	return nil
}
